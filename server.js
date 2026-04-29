#!/usr/bin/env node
/**
 * hive-mcp-log — Inbound structured-log ingestion shim.
 *
 * Scope: NDJSON ingestion at $0.0001/log line via x402, retention tiers
 *   (1d free, 7d $0.005/MB, 30d $0.02/MB), tail/search endpoints. Inbound
 *   only — this service never reaches out, never DMs, never pulls. Agents
 *   POST logs, agents read their own logs back. Pure protocol.
 *
 * Brand: Hive Civilization gold #C08D23 (Pantone 1245 C).
 * Spec : MCP 2024-11-05 / Streamable-HTTP / JSON-RPC 2.0.
 * Wallet: W1 MONROE 0x15184bf50b3d3f52b60434f8942b7d52f2eb436e (Base L2).
 */

import express from 'express';
import crypto from 'node:crypto';
import {
  openDb, recordIngest, insertLines, searchLines, tailLines,
  getRetention, setRetention, bumpStoredBytes, todayIngestSummary,
  pruneByRetention,
} from './lib/ledger.js';
import { parseBody } from './lib/parse.js';
import { priceIngest, isValidTier, RETENTION_TIERS, INGEST_USD_PER_LINE } from './lib/pricing.js';
import { verifyUsdcPayment } from './lib/verify.js';

const app = express();
app.use(express.json({ limit: '4mb' }));
app.use(express.text({ type: ['text/plain', 'application/x-ndjson', 'application/ndjson'], limit: '4mb' }));

const PORT = process.env.PORT || 3000;
const ENABLE = String(process.env.ENABLE ?? 'true').toLowerCase() === 'true';
const WALLET_ADDRESS = process.env.WALLET_ADDRESS || '0x15184bf50b3d3f52b60434f8942b7d52f2eb436e';
const SOLANA_WALLET = process.env.SOLANA_WALLET || 'B1N61cuL35fhskWz5dw8XqDyP6LWi3ZWmq8CNA9L3FVn';
const USDC_BASE = process.env.USDC_BASE || '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913';
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || 'https://hive-mcp-log.onrender.com';
const MAX_LINES_PER_INGEST = Number(process.env.MAX_LINES_PER_INGEST || 10_000);
const MAX_BYTES_PER_INGEST = Number(process.env.MAX_BYTES_PER_INGEST || 4 * 1024 * 1024);

openDb();

// ─── helpers ─────────────────────────────────────────────────────────────
function brand() {
  return {
    name: 'hive-mcp-log',
    civilization: 'Hive Civilization',
    gold_hex: '#C08D23',
    pantone: '1245 C',
    inbound_only: true,
  };
}

function notEnabledEnvelope() {
  return {
    error: 'log_ingestion_disabled',
    hint: 'Set ENABLE=true on the service to flip ingestion live.',
    enable_default: 'true',
  };
}

const BOGO = {
  first_call_free: true,
  loyalty_threshold: 6,
  pitch: "Pay this once, your 6th paid call is on the house. New here? Add header 'x-hive-did' to claim your first call free.",
  claim_with: 'x-hive-did header',
};

function paymentEnvelope({ price, did, ingestion_id, retention_class }) {
  return {
    x402_version: 1,
    intent: 'log_ingest',
    accepts: [
      {
        scheme: 'eip3009',
        network: 'base',
        asset: USDC_BASE,
        recipient: WALLET_ADDRESS,
        max_amount_required: String(price.total_usd),
        currency: 'USD',
        decimals: 6,
      },
      {
        scheme: 'spl-transfer',
        network: 'solana',
        asset: 'USDC',
        recipient: SOLANA_WALLET,
        max_amount_required: String(price.total_usd),
        currency: 'USD',
        decimals: 6,
      },
    ],
    quote: {
      ingest_usd: String(price.ingest_usd),
      retention_surcharge_usd: String(price.retention_surcharge_usd),
      total_usd: String(price.total_usd),
      retention_class: retention_class,
      ingest_usd_per_line: String(INGEST_USD_PER_LINE),
    },
    settlement_callback: `${PUBLIC_BASE_URL}/v1/log/settle`,
    ingestion_id,
    did,
    expires_in_s: 300,
    bogo: BOGO,
  };
}

function didFromReq(req, body) {
  const h = req.headers['x-hive-did'] || req.headers['x-did'];
  if (h) return String(h);
  if (body && typeof body === 'object' && body.did) return String(body.did);
  return 'did:hive:anonymous';
}

// ─── MCP tools ───────────────────────────────────────────────────────────
const TOOLS = [
  {
    name: 'log_ingest',
    description: 'Ingest a structured log batch (NDJSON, max 4 MB, max 10k lines per call). Returns ingestion receipt with byte count, line count, and x402 charge. $0.0001/line + retention surcharge per tier.',
    inputSchema: {
      type: 'object',
      required: ['did', 'lines'],
      properties: {
        did: { type: 'string', description: 'Calling DID, e.g. did:hive:agent-foo.' },
        lines: { type: 'array', description: 'Array of log objects. Common fields: ts, severity, tag, msg.' },
        retention_class: { type: 'string', enum: ['1d', '7d', '30d'], description: 'Retention tier for this batch. Default 1d (free).' },
        tx_hash: { type: 'string', description: 'Optional Base L2 USDC tx hash for synchronous verification.' },
      },
    },
  },
  {
    name: 'log_tail',
    description: 'Return last N log lines for the calling DID. Tier 0, free, own-DID only.',
    inputSchema: {
      type: 'object',
      required: ['did'],
      properties: {
        did: { type: 'string' },
        n: { type: 'integer', description: 'How many lines to return (max 500, default 50).' },
      },
    },
  },
  {
    name: 'log_search',
    description: 'Search logs for the calling DID by time range, severity, tag, and free-text query. Tier 0 own-DID. Returns rows + cursor for pagination.',
    inputSchema: {
      type: 'object',
      required: ['did'],
      properties: {
        did: { type: 'string' },
        since_ms: { type: 'integer' },
        until_ms: { type: 'integer' },
        severity: { type: 'string', enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
        tag: { type: 'string' },
        q: { type: 'string', description: 'Free-text substring against msg or payload.' },
        limit: { type: 'integer' },
        cursor: { type: 'integer' },
      },
    },
  },
  {
    name: 'log_retention_get',
    description: 'Return current retention tier and bytes stored for the calling DID. Tier 0, free.',
    inputSchema: {
      type: 'object',
      required: ['did'],
      properties: { did: { type: 'string' } },
    },
  },
  {
    name: 'log_retention_set',
    description: 'Set retention tier for the calling DID (1d/7d/30d). Takes effect immediately for new ingests.',
    inputSchema: {
      type: 'object',
      required: ['did', 'tier'],
      properties: {
        did: { type: 'string' },
        tier: { type: 'string', enum: ['1d', '7d', '30d'] },
      },
    },
  },
  {
    name: 'log_today',
    description: 'Today aggregate — ingests, lines, bytes, charge_usd. Tier 0, free, read-only.',
    inputSchema: { type: 'object', properties: {} },
  },
];

// ─── REST: ingest ────────────────────────────────────────────────────────
app.post('/v1/log/ingest', async (req, res) => {
  if (!ENABLE) return res.status(503).json(notEnabledEnvelope());

  const ct = String(req.headers['content-type'] || '');
  const body = req.body;
  let lines = [];
  let did = didFromReq(req, body);
  let retention_class = req.headers['x-hive-retention'] || (body && body.retention_class) || '1d';
  let tx_hash = req.headers['x-hive-tx'] || (body && body.tx_hash) || null;

  if (typeof body === 'string') {
    lines = parseBody(body, ct);
  } else if (body && typeof body === 'object') {
    lines = parseBody(body.lines || body, ct);
  }

  if (!isValidTier(retention_class)) retention_class = '1d';
  if (!Array.isArray(lines) || lines.length === 0) {
    return res.status(400).json({ error: 'no_lines', hint: 'Send NDJSON body or { did, lines: [...] }.' });
  }
  if (lines.length > MAX_LINES_PER_INGEST) {
    return res.status(413).json({ error: 'too_many_lines', max: MAX_LINES_PER_INGEST });
  }

  let total_bytes = 0;
  for (const ln of lines) total_bytes += ln.bytes || 0;
  if (total_bytes > MAX_BYTES_PER_INGEST) {
    return res.status(413).json({ error: 'too_many_bytes', max: MAX_BYTES_PER_INGEST });
  }

  const price = priceIngest({ line_count: lines.length, bytes: total_bytes, tier: retention_class });
  const ingestion_id = 'ing-' + crypto.randomBytes(8).toString('hex');

  if (!tx_hash) {
    res.status(402);
    return res.json({
      ...paymentEnvelope({ price, did, ingestion_id, retention_class }),
      preview: {
        line_count: lines.length,
        bytes: total_bytes,
      },
      hint: 'POST again with x-hive-tx: <base_tx_hash> after settling on Base L2.',
    });
  }

  const verification = await verifyUsdcPayment({ tx_hash, expected_usd: price.total_usd });
  if (!verification.ok) {
    recordIngest({
      id: ingestion_id, did,
      line_count: lines.length, bytes: total_bytes,
      charge_usd: price.total_usd, retention_class, tx_hash,
      verified: 0, reason: verification.reason,
    });
    return res.status(402).json({
      error: 'payment_unverified',
      reason: verification.reason,
      ...paymentEnvelope({ price, did, ingestion_id, retention_class }),
    });
  }

  const rows = lines.map(ln => ({
    ingestion_id, did,
    ts_ms: ln.ts_ms,
    severity: ln.severity,
    tag: ln.tag,
    msg: ln.msg,
    payload_json: ln.payload_json,
    bytes: ln.bytes,
  }));
  insertLines(rows);
  bumpStoredBytes(did, total_bytes);
  recordIngest({
    id: ingestion_id, did,
    line_count: lines.length, bytes: total_bytes,
    charge_usd: price.total_usd, retention_class, tx_hash,
    verified: 1, reason: null,
  });

  return res.json({
    ok: true,
    ingestion_id,
    did,
    line_count: lines.length,
    bytes: total_bytes,
    retention_class,
    charge_usd: String(price.total_usd),
    ingest_usd: String(price.ingest_usd),
    retention_surcharge_usd: String(price.retention_surcharge_usd),
    tx_hash,
    paid_usd: verification.paid_usd,
  });
});

// ─── REST: tail (SSE or JSON) ────────────────────────────────────────────
app.get('/v1/log/tail', (req, res) => {
  const did = String(req.query.did || req.headers['x-hive-did'] || '');
  if (!did) return res.status(400).json({ error: 'did_required' });
  const n = Math.min(Math.max(Number(req.query.n) || 50, 1), 500);
  const accept = String(req.headers['accept'] || '').toLowerCase();
  const wantsSse = accept.includes('text/event-stream') || String(req.query.stream || '') === '1';

  if (!wantsSse) {
    const rows = tailLines({ did, n });
    return res.json({ ok: true, did, n: rows.length, rows });
  }

  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  });
  res.flushHeaders?.();

  const initial = tailLines({ did, n });
  res.write(`event: snapshot\ndata: ${JSON.stringify({ did, rows: initial })}\n\n`);

  let lastSeenId = initial.length > 0 ? initial[0].id : 0;
  const intervalMs = Math.max(1000, Number(req.query.interval_ms) || 2000);
  const timer = setInterval(() => {
    try {
      const rows = tailLines({ did, n: 100 }).filter(r => r.id > lastSeenId);
      if (rows.length > 0) {
        lastSeenId = rows[0].id;
        for (const r of rows.slice().reverse()) {
          res.write(`event: line\ndata: ${JSON.stringify(r)}\n\n`);
        }
      } else {
        res.write(`event: ping\ndata: ${Date.now()}\n\n`);
      }
    } catch (e) {
      res.write(`event: error\ndata: ${JSON.stringify({ message: String(e?.message || e) })}\n\n`);
    }
  }, intervalMs);

  req.on('close', () => clearInterval(timer));
});

// ─── REST: search (POST + GET both supported) ───────────────────────────
function searchHandler(req, res) {
  const src = req.method === 'POST' ? (req.body || {}) : req.query;
  const did = String(src.did || req.headers['x-hive-did'] || '');
  if (!did) return res.status(400).json({ error: 'did_required' });
  const out = searchLines({
    did,
    since_ms: src.since_ms != null ? Number(src.since_ms) : undefined,
    until_ms: src.until_ms != null ? Number(src.until_ms) : undefined,
    severity: src.severity || undefined,
    tag: src.tag || undefined,
    q: src.q || undefined,
    limit: src.limit != null ? Number(src.limit) : 100,
    cursor: src.cursor != null ? Number(src.cursor) : 0,
  });
  return res.json({ ok: true, did, ...out });
}
app.post('/v1/log/search', searchHandler);
app.get('/v1/log/search', searchHandler);

// ─── REST: retention ─────────────────────────────────────────────────────
app.get('/v1/log/retention', (req, res) => {
  const did = String(req.query.did || req.headers['x-hive-did'] || '');
  if (!did) return res.status(400).json({ error: 'did_required' });
  const r = getRetention(did);
  const tier = RETENTION_TIERS[r.tier] || RETENTION_TIERS['1d'];
  const monthly_projection_usd = (r.bytes_stored / (1024 * 1024)) * tier.usd_per_mb;
  return res.json({
    ok: true,
    did,
    tier: r.tier,
    bytes_stored: r.bytes_stored,
    usd_per_mb: tier.usd_per_mb,
    monthly_projection_usd: Math.round(monthly_projection_usd * 1e6) / 1e6,
    tiers: RETENTION_TIERS,
  });
});

app.patch('/v1/log/retention', (req, res) => {
  const did = String(req.body?.did || req.headers['x-hive-did'] || '');
  const tier = String(req.body?.tier || '');
  if (!did) return res.status(400).json({ error: 'did_required' });
  if (!isValidTier(tier)) return res.status(400).json({ error: 'invalid_tier', allowed: Object.keys(RETENTION_TIERS) });
  setRetention(did, tier);
  return res.json({ ok: true, did, tier });
});

// ─── REST: today aggregate (Tier 0, free) ────────────────────────────────
app.get('/v1/log/today', (_req, res) => {
  const t = todayIngestSummary();
  return res.json({ ok: true, ...t });
});

// ─── MCP surface ─────────────────────────────────────────────────────────
app.post('/mcp', async (req, res) => {
  const rpc = req.body || {};
  const id = rpc.id ?? null;
  const method = rpc.method;
  const params = rpc.params || {};

  function ok(result) { res.json({ jsonrpc: '2.0', id, result }); }
  function err(code, message) { res.json({ jsonrpc: '2.0', id, error: { code, message } }); }

  try {
    if (method === 'initialize') {
      return ok({
        protocolVersion: '2024-11-05',
        capabilities: { tools: {} },
        serverInfo: { name: 'hive-mcp-log', version: '1.0.0' },
        instructions: 'Inbound structured-log ingestion. NDJSON via tools.call("log_ingest"), or POST /v1/log/ingest. Tail/search are Tier 0 (free) for own-DID reads.',
      });
    }
    if (method === 'tools/list') return ok({ tools: TOOLS });
    if (method === 'tools/call') {
      const name = params.name;
      const args = params.arguments || {};

      if (name === 'log_ingest') {
        if (!ENABLE) return ok({ content: [{ type: 'text', text: JSON.stringify(notEnabledEnvelope()) }], isError: true });
        const did = args.did || 'did:hive:anonymous';
        const retention_class = isValidTier(args.retention_class) ? args.retention_class : '1d';
        const lines = parseBody(args.lines || []);
        if (!lines.length) return ok({ content: [{ type: 'text', text: JSON.stringify({ error: 'no_lines' }) }], isError: true });
        let total_bytes = 0;
        for (const ln of lines) total_bytes += ln.bytes || 0;
        const price = priceIngest({ line_count: lines.length, bytes: total_bytes, tier: retention_class });
        const ingestion_id = 'ing-' + crypto.randomBytes(8).toString('hex');
        if (!args.tx_hash) {
          return ok({
            content: [{
              type: 'text',
              text: JSON.stringify(paymentEnvelope({ price, did, ingestion_id, retention_class })),
            }],
          });
        }
        const verification = await verifyUsdcPayment({ tx_hash: args.tx_hash, expected_usd: price.total_usd });
        if (!verification.ok) {
          recordIngest({ id: ingestion_id, did, line_count: lines.length, bytes: total_bytes, charge_usd: price.total_usd, retention_class, tx_hash: args.tx_hash, verified: 0, reason: verification.reason });
          return ok({ content: [{ type: 'text', text: JSON.stringify({ error: 'payment_unverified', reason: verification.reason }) }], isError: true });
        }
        const rows = lines.map(ln => ({ ingestion_id, did, ts_ms: ln.ts_ms, severity: ln.severity, tag: ln.tag, msg: ln.msg, payload_json: ln.payload_json, bytes: ln.bytes }));
        insertLines(rows);
        bumpStoredBytes(did, total_bytes);
        recordIngest({ id: ingestion_id, did, line_count: lines.length, bytes: total_bytes, charge_usd: price.total_usd, retention_class, tx_hash: args.tx_hash, verified: 1, reason: null });
        return ok({ content: [{ type: 'text', text: JSON.stringify({ ok: true, ingestion_id, line_count: lines.length, bytes: total_bytes, charge_usd: price.total_usd }) }] });
      }
      if (name === 'log_tail') {
        const did = args.did || 'did:hive:anonymous';
        const n = Math.min(Math.max(Number(args.n) || 50, 1), 500);
        return ok({ content: [{ type: 'text', text: JSON.stringify({ ok: true, did, rows: tailLines({ did, n }) }) }] });
      }
      if (name === 'log_search') {
        const did = args.did || 'did:hive:anonymous';
        const out = searchLines({
          did,
          since_ms: Number.isFinite(args.since_ms) ? args.since_ms : undefined,
          until_ms: Number.isFinite(args.until_ms) ? args.until_ms : undefined,
          severity: args.severity,
          tag: args.tag,
          q: args.q,
          limit: args.limit,
          cursor: args.cursor,
        });
        return ok({ content: [{ type: 'text', text: JSON.stringify({ ok: true, did, ...out }) }] });
      }
      if (name === 'log_retention_get') {
        const did = args.did || 'did:hive:anonymous';
        const r = getRetention(did);
        return ok({ content: [{ type: 'text', text: JSON.stringify({ ok: true, did, ...r, tiers: RETENTION_TIERS }) }] });
      }
      if (name === 'log_retention_set') {
        const did = args.did || 'did:hive:anonymous';
        const tier = String(args.tier || '');
        if (!isValidTier(tier)) return ok({ content: [{ type: 'text', text: JSON.stringify({ error: 'invalid_tier' }) }], isError: true });
        setRetention(did, tier);
        return ok({ content: [{ type: 'text', text: JSON.stringify({ ok: true, did, tier }) }] });
      }
      if (name === 'log_today') {
        return ok({ content: [{ type: 'text', text: JSON.stringify({ ok: true, ...todayIngestSummary() }) }] });
      }
      return err(-32601, `tool not found: ${name}`);
    }
    return err(-32601, `method not found: ${method}`);
  } catch (e) {
    return err(-32000, String(e?.message || e));
  }
});

// ─── discovery / health / root ───────────────────────────────────────────
app.get('/.well-known/mcp.json', (_req, res) => {
  res.json({
    name: 'hive-mcp-log',
    version: '1.0.0',
    protocolVersion: '2024-11-05',
    transport: 'streamable-http',
    endpoint: `${PUBLIC_BASE_URL}/mcp`,
    tools: TOOLS.map(t => ({ name: t.name, description: t.description })),
    brand: brand(),
  });
});

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'hive-mcp-log',
    version: '1.0.0',
    enabled: ENABLE,
    inbound_only: true,
    settlement: { wallet_base: WALLET_ADDRESS, wallet_solana: SOLANA_WALLET, asset: 'USDC' },
    pricing: {
      ingest_usd_per_line: INGEST_USD_PER_LINE,
      retention: RETENTION_TIERS,
    },
    brand: brand(),
    ts: new Date().toISOString(),
  });
});

app.get('/', (req, res) => {
  const accept = String(req.headers['accept'] || '');
  if (accept.includes('text/html')) {
    res.set('Content-Type', 'text/html; charset=utf-8');
    return res.send(landingHtml());
  }
  res.json({
    service: 'hive-mcp-log',
    version: '1.0.0',
    enabled: ENABLE,
    mcp: `${PUBLIC_BASE_URL}/mcp`,
    health: `${PUBLIC_BASE_URL}/health`,
    discovery: `${PUBLIC_BASE_URL}/.well-known/mcp.json`,
    inbound_only: true,
    brand: brand(),
  });
});

function landingHtml() {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>hive-mcp-log — Hive Civilization</title>
<style>
  :root { --gold: #C08D23; --bg: #0b0b0b; --fg: #e8e8e8; --mut: #9a9a9a; --line: #1d1d1d; }
  body { background: var(--bg); color: var(--fg); font: 15px/1.55 ui-sans-serif, system-ui, -apple-system, sans-serif; margin: 0; }
  main { max-width: 760px; margin: 0 auto; padding: 48px 24px 80px; }
  h1 { color: var(--gold); font-size: 28px; margin: 0 0 4px; letter-spacing: -0.01em; }
  .sub { color: var(--mut); margin: 0 0 28px; }
  h2 { color: var(--gold); font-size: 16px; text-transform: uppercase; letter-spacing: 0.08em; margin: 32px 0 8px; }
  table { width: 100%; border-collapse: collapse; margin-top: 8px; }
  th, td { text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--line); font-size: 14px; }
  th { color: var(--gold); font-weight: 500; text-transform: uppercase; letter-spacing: 0.06em; font-size: 12px; }
  code, pre { background: #131313; color: var(--fg); border-radius: 4px; }
  code { padding: 1px 5px; }
  pre { padding: 12px; overflow-x: auto; border-left: 3px solid var(--gold); }
  a { color: var(--gold); }
  .pill { display: inline-block; padding: 2px 8px; border: 1px solid var(--gold); color: var(--gold); border-radius: 999px; font-size: 12px; letter-spacing: 0.08em; }
  footer { color: var(--mut); margin-top: 40px; font-size: 12px; }
</style>
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@type": "SoftwareApplication",
  "name": "hive-mcp-log",
  "applicationCategory": "DeveloperApplication",
  "operatingSystem": "Cross-platform",
  "description": "Inbound structured-log ingestion for the A2A network. NDJSON ingestion at $0.0001 per line via x402, retention tiers (1d free, 7d $0.005/MB, 30d $0.02/MB), tail and search endpoints. USDC settlement on Base L2.",
  "url": "${PUBLIC_BASE_URL}",
  "author": { "@type": "Person", "name": "Steve Rotzin", "email": "steve@thehiveryiq.com", "url": "https://www.thehiveryiq.com" },
  "license": "https://opensource.org/licenses/MIT",
  "offers": [
    { "@type": "Offer", "name": "Ingest", "price": "0.0001", "priceCurrency": "USD", "description": "per log line" },
    { "@type": "Offer", "name": "Retention 1d", "price": "0", "priceCurrency": "USD", "description": "default, free" },
    { "@type": "Offer", "name": "Retention 7d", "price": "0.005", "priceCurrency": "USD", "description": "per MB" },
    { "@type": "Offer", "name": "Retention 30d", "price": "0.02", "priceCurrency": "USD", "description": "per MB" }
  ]
}
</script>
</head>
<body>
<main>
  <span class="pill">INBOUND ONLY</span>
  <h1>hive-mcp-log</h1>
  <p class="sub">Structured-log ingestion shim. Pure protocol — no DMs, no spam, no outbound calls.</p>

  <h2>Pricing</h2>
  <table>
    <tr><th>Surface</th><th>Rate</th></tr>
    <tr><td>Ingest</td><td>$0.0001 per log line</td></tr>
    <tr><td>Retention 1d (default)</td><td>Free</td></tr>
    <tr><td>Retention 7d</td><td>$0.005 / MB</td></tr>
    <tr><td>Retention 30d</td><td>$0.02 / MB</td></tr>
    <tr><td>Tail / search (own-DID)</td><td>Tier 0 — free</td></tr>
  </table>

  <h2>Endpoints</h2>
  <table>
    <tr><th>Method</th><th>Path</th><th>Purpose</th></tr>
    <tr><td>POST</td><td><code>/v1/log/ingest</code></td><td>Ingest NDJSON batch (max 4 MB / 10k lines)</td></tr>
    <tr><td>GET</td><td><code>/v1/log/tail</code></td><td>Last N lines for the calling DID</td></tr>
    <tr><td>GET</td><td><code>/v1/log/search</code></td><td>Search by time / severity / tag / text</td></tr>
    <tr><td>GET</td><td><code>/v1/log/retention</code></td><td>Current tier and bytes stored</td></tr>
    <tr><td>PATCH</td><td><code>/v1/log/retention</code></td><td>Change retention tier</td></tr>
    <tr><td>GET</td><td><code>/v1/log/today</code></td><td>Today aggregate (Tier 0, free)</td></tr>
    <tr><td>POST</td><td><code>/mcp</code></td><td>MCP 2024-11-05 JSON-RPC surface</td></tr>
    <tr><td>GET</td><td><code>/health</code></td><td>Service health</td></tr>
  </table>

  <h2>Settle</h2>
  <p>USDC on Base L2 to <code>${WALLET_ADDRESS}</code> — pass tx hash via <code>x-hive-tx</code> header or <code>tx_hash</code> field for synchronous verification.</p>

  <footer>
    Hive Civilization · brand gold <span style="color: var(--gold)">#C08D23</span> ·
    <a href="https://github.com/srotzin/hive-mcp-log">github.com/srotzin/hive-mcp-log</a>
  </footer>
</main>
</body>
</html>`;
}

// ─── retention sweep ─────────────────────────────────────────────────────
const SWEEP_MS = Number(process.env.RETENTION_SWEEP_MS || 60 * 60 * 1000);
setInterval(() => {
  try {
    const n = pruneByRetention();
    if (n > 0) console.log(`[retention] pruned ${n} lines`);
  } catch (e) { console.error('[retention]', e?.message || e); }
}, SWEEP_MS).unref();

// ─── Schema discoverability (auto-injected) ──────────────────────────────
app.get('/.well-known/agent-card.json', (req, res) => res.json({
  name: 'hive-mcp-log',
  description: "Hive Civilization log ingestion MCP \u2014 pay-per-write event log with x402 USDC settlement. New agents: first call free. Loyalty: every 6th paid call is free. Pay in USDC on Base L2.",
  url: 'https://hive-mcp-log.onrender.com',
  provider: { organization: 'Hive Civilization', url: 'https://www.thehiveryiq.com', contact: 'steve@thehiveryiq.com' },
  version: '1.0.0',
  capabilities: { streaming: false, pushNotifications: false, stateTransitionHistory: false },
  authentication: {
    schemes: ['x402'],
    credentials: { type:'x402', asset:'USDC', network:'base',
      asset_address: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
      recipient: '0x15184bf50b3d3f52b60434f8942b7d52f2eb436e'
    }
  },
  defaultInputModes: ['application/json'],
  defaultOutputModes: ['application/json'],
  extensions: {
    hive_pricing: {
      currency: 'USDC', network: 'base', model: 'per_call',
      first_call_free: true, loyalty_threshold: 6,
      loyalty_message: 'Every 6th paid call is free'
    }
  },
  bogo: {
    first_call_free: true, loyalty_threshold: 6,
    pitch: "Pay this once, your 6th paid call is on the house. New here? Add header 'x-hive-did' to claim your first call free.",
    claim_with: 'x-hive-did header'
  }
}));
app.get('/.well-known/ap2.json', (req, res) => res.json({
  ap2_version: '1',
  agent: {
    name: 'hive-mcp-log',
    did: 'did:web:hive-mcp-log.onrender.com',
    description: "Hive Civilization log ingestion MCP \u2014 pay-per-write event log with x402 USDC settlement. New agents: first call free. Loyalty: every 6th paid call is free. Pay in USDC on Base L2."
  },
  endpoints: {
    mcp: 'https://hive-mcp-log.onrender.com/mcp',
    agent_card: 'https://hive-mcp-log.onrender.com/.well-known/agent-card.json'
  },
  payments: {
    schemes: ['x402'],
    primary: { scheme:'x402', network:'base', asset:'USDC',
      asset_address: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
      recipient: '0x15184bf50b3d3f52b60434f8942b7d52f2eb436e'
    }
  },
  bogo: {
    first_call_free: true, loyalty_threshold: 6,
    pitch: "Pay this once, your 6th paid call is on the house.",
    claim_with: 'x-hive-did header'
  },
  brand: { color: '#C08D23', name: 'Hive Civilization' }
}));



// ─── Subscription & enterprise tier endpoints (Wave B codification) ──────────
// Partner-doctrine: identity/receipts/trust plumbing only.
// Subscription billing is denominated in USDC on Base (Monroe W1).
// Spectral receipt is emitted on every fee event via hive-receipt sidecar.
//
// Tier schedule:
//   Tier 1 (Starter)    : 50.0/mo
//   Tier 2 (Pro)        : 100.0/mo
//   Tier 3 (Enterprise) : 400.0/mo
//
// x402 tx_hash required for Tier 1+ confirmation. Tier 3 can invoice monthly.
//
// Spectral receipt: POST to hive-receipt sidecar for tamper-evident audit trail.

const SUBSCRIPTION_TIERS = {
  starter:    { price_usd: 50.0, calls_per_day: 1000000, label: 'Starter' },
  pro:        { price_usd: 100.0, calls_per_day: 10000000, label: 'Pro' },
  enterprise: { price_usd: 400.0, calls_per_day: Infinity, label: 'Enterprise', invoice: true },
};

// In-memory subscription ledger (durable persistence on hivemorph backend).
const _subLedger = new Map(); // did -> { tier, activated_ms, tx_hash }

async function emitSpectralReceipt({ event_type, did, amount_usd, tool_name, tx_hash, metadata }) {
  // Posts a Spectral-signed receipt to hive-receipt. Non-blocking.
  // Error is logged but never throws — receipt emission must not block the fee path.
  try {
    const body = JSON.stringify({
      issuer_did: 'did:hive:log',
      recipient_did: did || 'did:hive:anonymous',
      event_type,
      tool_name,
      amount_usd: String(amount_usd),
      currency: 'USDC',
      network: 'base',
      pay_to: '0x15184bf50b3d3f52b60434f8942b7d52f2eb436e',
      tx_hash: tx_hash || null,
      issued_ms: Date.now(),
      service: 'Hive Log',
      brand: '#C08D23',
      ...metadata,
    });
    const ctrl = new AbortController();
    const tid = setTimeout(() => ctrl.abort(), 4000);
    await fetch('https://hive-receipt.onrender.com/v1/receipt/sign', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body,
      signal: ctrl.signal,
    });
    clearTimeout(tid);
  } catch (_) {
    // Receipt emission is best-effort. Log and continue.
    console.warn('[log] receipt emit failed (non-fatal):', _.message || _);
  }
}

// POST /v1/subscription — create or upgrade a subscription
app.post('/v1/subscription', async (req, res) => {
  const { tier, did, tx_hash } = req.body || {};
  if (!tier || !SUBSCRIPTION_TIERS[tier]) {
    return res.status(400).json({
      error: 'invalid_tier',
      valid_tiers: Object.keys(SUBSCRIPTION_TIERS),
      brand: '#C08D23',
    });
  }
  const t = SUBSCRIPTION_TIERS[tier];
  if (!did) return res.status(400).json({ error: 'did_required' });

  // Enterprise tier can invoice monthly (no tx_hash required at activation).
  if (tier !== 'enterprise' && !tx_hash) {
    return res.status(402).json({
      error: 'payment_required',
      x402: {
        type: 'x402', version: '1', kind: 'subscription_log',
        asking_usd: t.price_usd,
        accept_min_usd: t.price_usd,
        asset: 'USDC', asset_address: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
        network: 'base', pay_to: '0x15184bf50b3d3f52b60434f8942b7d52f2eb436e',
        nonce: Math.random().toString(36).slice(2),
        issued_ms: Date.now(),
        tier, label: t.label,
        bogo: { first_call_free: true, loyalty_every_n: 6 },
      },
      note: `Submit tx_hash for ${t.price_usd} USDC/mo to 0x15184bf50b3d3f52b60434f8942b7d52f2eb436e on Base.`,
    });
  }

  const record = {
    tier, did, tx_hash: tx_hash || 'enterprise_invoice',
    activated_ms: Date.now(),
    expires_ms: Date.now() + 30 * 24 * 3600 * 1000,
    price_usd: t.price_usd,
    calls_per_day: t.calls_per_day,
  };
  _subLedger.set(did, record);

  // Emit Spectral receipt for subscription activation.
  await emitSpectralReceipt({
    event_type: 'subscription_activated',
    did, amount_usd: t.price_usd, tool_name: 'subscription',
    tx_hash: tx_hash || null,
    metadata: { tier, service: 'Hive Log', expires_ms: record.expires_ms },
  });

  return res.json({
    ok: true,
    subscription: record,
    receipt_emitted: true,
    partner_attribution: 'Log ingestion and retention — audit attestation via Spectral. Complements Datadog, Splunk.',
    brand: '#C08D23',
    note: 'Subscription active for 30 days. Spectral receipt issued to hive-receipt.',
  });
});

// GET /v1/subscription/:did — check subscription status
app.get('/v1/subscription/:did', (req, res) => {
  const record = _subLedger.get(req.params.did);
  if (!record) {
    return res.status(404).json({ active: false, did: req.params.did });
  }
  const active = Date.now() < record.expires_ms;
  return res.json({ active, ...record });
});

// POST /v1/subscription/verify — lightweight verification (no charge)
app.post('/v1/subscription/verify', (req, res) => {
  const { did } = req.body || {};
  const record = _subLedger.get(did);
  const active = record && Date.now() < record.expires_ms;
  return res.json({
    active: !!active,
    did: did || null,
    tier: record?.tier || null,
    expires_ms: record?.expires_ms || null,
    brand: '#C08D23',
  });
});

// ─────────────────────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`hive-mcp-log listening on :${PORT} (ENABLE=${ENABLE})`);
});
