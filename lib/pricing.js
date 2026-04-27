/**
 * Pricing model for hive-mcp-log.
 *
 * Ingest:    $0.0001 per line (rounded to 6 decimals).
 * Retention: 1d free, 7d $0.005/MB-month, 30d $0.02/MB-month — surcharge
 *            assessed on stored MB at the configured tier and added on top
 *            of ingest charge for the lines being persisted.
 *
 * Surcharge convention: the per-ingest retention surcharge is the per-MB
 * tier rate times the bytes being added (in MB), prorated to the ingest
 * event itself (one-shot, not month-recurring) — keeps settlement
 * synchronous and avoids monthly billing reconciliation.
 */

export const INGEST_USD_PER_LINE = 0.0001;

export const RETENTION_TIERS = {
  '1d': { ms: 1 * 24 * 3600 * 1000, usd_per_mb: 0 },
  '7d': { ms: 7 * 24 * 3600 * 1000, usd_per_mb: 0.005 },
  '30d': { ms: 30 * 24 * 3600 * 1000, usd_per_mb: 0.02 },
};

export function isValidTier(t) { return Object.prototype.hasOwnProperty.call(RETENTION_TIERS, t); }

export function priceIngest({ line_count, bytes, tier }) {
  const lc = Math.max(0, Number(line_count) || 0);
  const b = Math.max(0, Number(bytes) || 0);
  const t = isValidTier(tier) ? tier : '1d';
  const ingest = round6(lc * INGEST_USD_PER_LINE);
  const surcharge = round6((b / (1024 * 1024)) * RETENTION_TIERS[t].usd_per_mb);
  return {
    tier: t,
    ingest_usd: ingest,
    retention_surcharge_usd: surcharge,
    total_usd: round6(ingest + surcharge),
  };
}

function round6(n) { return Math.round(Number(n) * 1e6) / 1e6; }
