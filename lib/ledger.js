/**
 * SQLite ledger at /tmp/log.db. Tables: ingests, lines, retention, payments.
 *
 * /tmp survives Render restart for the lifetime of the instance. Lines older
 * than the calling DID's retention window are pruned by a periodic sweep.
 */

import Database from 'better-sqlite3';

const DB_PATH = process.env.LOG_DB || '/tmp/log.db';

let db;
export function openDb() {
  if (db) return db;
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS ingests (
      id TEXT PRIMARY KEY,
      did TEXT,
      ts TEXT,
      ts_ms INTEGER,
      line_count INTEGER,
      bytes INTEGER,
      charge_usd REAL,
      retention_class TEXT,
      tx_hash TEXT,
      verified INTEGER DEFAULT 0,
      reason TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_ingests_did ON ingests(did);
    CREATE INDEX IF NOT EXISTS idx_ingests_ts ON ingests(ts_ms);

    CREATE TABLE IF NOT EXISTS lines (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ingestion_id TEXT,
      did TEXT,
      ts_ms INTEGER,
      severity TEXT,
      tag TEXT,
      msg TEXT,
      payload_json TEXT,
      bytes INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_lines_did_ts ON lines(did, ts_ms);
    CREATE INDEX IF NOT EXISTS idx_lines_ingestion ON lines(ingestion_id);
    CREATE INDEX IF NOT EXISTS idx_lines_sev ON lines(did, severity);
    CREATE INDEX IF NOT EXISTS idx_lines_tag ON lines(did, tag);

    CREATE TABLE IF NOT EXISTS retention (
      did TEXT PRIMARY KEY,
      tier TEXT,
      bytes_stored INTEGER DEFAULT 0,
      updated_at TEXT
    );
  `);
  return db;
}

export function recordIngest({ id, did, line_count, bytes, charge_usd, retention_class, tx_hash, verified, reason }) {
  const d = openDb();
  const now = new Date();
  d.prepare(`
    INSERT INTO ingests (id, did, ts, ts_ms, line_count, bytes, charge_usd, retention_class, tx_hash, verified, reason)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(id, did, now.toISOString(), now.getTime(), line_count, bytes, charge_usd, retention_class, tx_hash || null, verified ? 1 : 0, reason || null);
}

export function insertLines(rows) {
  const d = openDb();
  const stmt = d.prepare(`
    INSERT INTO lines (ingestion_id, did, ts_ms, severity, tag, msg, payload_json, bytes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const tx = d.transaction((items) => {
    for (const r of items) {
      stmt.run(r.ingestion_id, r.did, r.ts_ms, r.severity || 'info', r.tag || null, r.msg || null, r.payload_json || null, r.bytes || 0);
    }
  });
  tx(rows);
}

export function searchLines({ did, since_ms, until_ms, severity, tag, q, limit = 100, cursor = 0 }) {
  const d = openDb();
  const where = ['did = ?'];
  const args = [did];
  if (Number.isFinite(since_ms)) { where.push('ts_ms >= ?'); args.push(since_ms); }
  if (Number.isFinite(until_ms)) { where.push('ts_ms <= ?'); args.push(until_ms); }
  if (severity) { where.push('severity = ?'); args.push(severity); }
  if (tag) { where.push('tag = ?'); args.push(tag); }
  if (q) { where.push('(msg LIKE ? OR payload_json LIKE ?)'); args.push(`%${q}%`, `%${q}%`); }
  if (Number.isFinite(cursor) && cursor > 0) { where.push('id < ?'); args.push(cursor); }
  args.push(Math.min(Number(limit) || 100, 500));
  const sql = `SELECT id, ingestion_id, ts_ms, severity, tag, msg, payload_json, bytes FROM lines WHERE ${where.join(' AND ')} ORDER BY id DESC LIMIT ?`;
  const rows = d.prepare(sql).all(...args);
  const next_cursor = rows.length > 0 ? rows[rows.length - 1].id : null;
  return { rows, next_cursor };
}

export function tailLines({ did, n = 50 }) {
  const d = openDb();
  const limit = Math.min(Math.max(Number(n) || 50, 1), 500);
  return d.prepare(`SELECT id, ingestion_id, ts_ms, severity, tag, msg, payload_json, bytes FROM lines WHERE did = ? ORDER BY id DESC LIMIT ?`).all(did, limit);
}

export function getRetention(did) {
  const d = openDb();
  const row = d.prepare(`SELECT did, tier, bytes_stored, updated_at FROM retention WHERE did = ?`).get(did);
  if (row) return row;
  return { did, tier: '1d', bytes_stored: 0, updated_at: null };
}

export function setRetention(did, tier) {
  const d = openDb();
  const now = new Date().toISOString();
  d.prepare(`
    INSERT INTO retention (did, tier, bytes_stored, updated_at) VALUES (?, ?, COALESCE((SELECT bytes_stored FROM retention WHERE did = ?), 0), ?)
    ON CONFLICT(did) DO UPDATE SET tier = excluded.tier, updated_at = excluded.updated_at
  `).run(did, tier, did, now);
}

export function bumpStoredBytes(did, delta) {
  const d = openDb();
  const now = new Date().toISOString();
  d.prepare(`
    INSERT INTO retention (did, tier, bytes_stored, updated_at) VALUES (?, ?, ?, ?)
    ON CONFLICT(did) DO UPDATE SET bytes_stored = retention.bytes_stored + excluded.bytes_stored, updated_at = excluded.updated_at
  `).run(did, '1d', delta, now);
}

export function todayIngestSummary() {
  const d = openDb();
  const since = new Date(); since.setUTCHours(0, 0, 0, 0);
  const row = d.prepare(`
    SELECT COUNT(*) AS ingests, COALESCE(SUM(line_count),0) AS lines, COALESCE(SUM(bytes),0) AS bytes, COALESCE(SUM(charge_usd),0) AS charge_usd
    FROM ingests WHERE ts_ms >= ?
  `).get(since.getTime());
  return {
    date: new Date().toISOString().slice(0, 10),
    ingests: row.ingests,
    lines: row.lines,
    bytes: row.bytes,
    charge_usd: Number(row.charge_usd || 0),
  };
}

export function pruneByRetention() {
  const d = openDb();
  const now = Date.now();
  const tiers = d.prepare(`SELECT did, tier FROM retention`).all();
  let pruned = 0;
  for (const { did, tier } of tiers) {
    const ms = retentionMs(tier);
    if (!ms) continue;
    const cutoff = now - ms;
    const r = d.prepare(`DELETE FROM lines WHERE did = ? AND ts_ms < ?`).run(did, cutoff);
    pruned += r.changes;
  }
  return pruned;
}

export function retentionMs(tier) {
  if (tier === '30d') return 30 * 24 * 3600 * 1000;
  if (tier === '7d') return 7 * 24 * 3600 * 1000;
  return 1 * 24 * 3600 * 1000;
}
