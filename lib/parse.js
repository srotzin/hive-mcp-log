/**
 * NDJSON parser + line normalization for log ingestion.
 *
 * Accepts NDJSON (one JSON object per line) or a single JSON array body. Each
 * line is normalized to { ts_ms, severity, tag, msg, payload_json, bytes }.
 * Unknown shapes are accepted with msg = String(line) — we never reject a
 * payload, we just charge for it.
 */

const SEVERITIES = new Set(['debug', 'info', 'warn', 'error', 'fatal']);

export function parseBody(body, contentType = '') {
  if (Array.isArray(body)) {
    return body.map(o => normalizeOne(o)).filter(Boolean);
  }
  if (typeof body === 'object' && body && Array.isArray(body.lines)) {
    return body.lines.map(o => normalizeOne(o)).filter(Boolean);
  }
  if (typeof body === 'string') {
    return body.split(/\r?\n/).map(s => s.trim()).filter(Boolean).map(normalizeFromString).filter(Boolean);
  }
  if (body && typeof body === 'object') {
    const one = normalizeOne(body);
    return one ? [one] : [];
  }
  return [];
}

function normalizeFromString(s) {
  try {
    const obj = JSON.parse(s);
    return normalizeOne(obj, s);
  } catch (_e) {
    return {
      ts_ms: Date.now(),
      severity: 'info',
      tag: null,
      msg: s,
      payload_json: null,
      bytes: Buffer.byteLength(s, 'utf8'),
    };
  }
}

function normalizeOne(obj, raw = null) {
  if (!obj || typeof obj !== 'object') return null;
  const ts_ms = parseTs(obj.ts ?? obj.timestamp ?? obj.time ?? obj.ts_ms);
  const sev = String(obj.severity ?? obj.level ?? 'info').toLowerCase();
  const severity = SEVERITIES.has(sev) ? sev : 'info';
  const tag = obj.tag ?? obj.namespace ?? obj.component ?? null;
  const msg = obj.msg ?? obj.message ?? obj.text ?? null;
  const rest = { ...obj };
  delete rest.ts; delete rest.timestamp; delete rest.time; delete rest.ts_ms;
  delete rest.severity; delete rest.level; delete rest.tag; delete rest.namespace;
  delete rest.component; delete rest.msg; delete rest.message; delete rest.text;
  let payload_json = null;
  if (Object.keys(rest).length > 0) {
    try { payload_json = JSON.stringify(rest); } catch (_e) { payload_json = null; }
  }
  let serialized;
  if (typeof raw === 'string') {
    serialized = raw;
  } else {
    try { serialized = JSON.stringify(obj); } catch (_e) { serialized = String(msg ?? ''); }
  }
  if (typeof serialized !== 'string') serialized = String(serialized ?? '');
  return {
    ts_ms,
    severity,
    tag: tag ? String(tag).slice(0, 128) : null,
    msg: msg == null ? null : String(msg).slice(0, 4096),
    payload_json,
    bytes: Buffer.byteLength(serialized, 'utf8'),
  };
}

function parseTs(v) {
  if (v == null) return Date.now();
  if (typeof v === 'number' && Number.isFinite(v)) {
    return v < 1e12 ? Math.floor(v * 1000) : Math.floor(v);
  }
  const d = new Date(v);
  const t = d.getTime();
  return Number.isFinite(t) ? t : Date.now();
}
