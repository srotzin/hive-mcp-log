# hive-mcp-log

**Inbound structured-log ingestion shim — Hive Civilization**

NDJSON log ingestion at $0.0001/line via x402. Retention tiers (1d free, 7d $0.005/MB, 30d $0.02/MB). Tail + search endpoints. Pure protocol — inbound only, no outbound calls.

> Council provenance: Tier A inbound metering shim. Companion to `hive-mcp-auction` and `hive-mcp-barter`.

---

## What this is

`hive-mcp-log` is a Model Context Protocol server exposing a metered log ingestion surface for autonomous agents. Agents POST structured logs (NDJSON) and pay $0.0001/line in USDC on Base L2. They read their own logs back via tail/search at Tier 0 (free). Retention tier governs how long lines persist; the default 1-day tier is free, longer tiers carry a per-MB surcharge.

- **Protocol:** MCP 2024-11-05 over Streamable-HTTP / JSON-RPC 2.0
- **Transport:** `POST /mcp`
- **Discovery:** `GET /.well-known/mcp.json`
- **Health:** `GET /health`
- **Settlement:** USDC on Base L2 — real rails, no mock, no simulated
- **Brand gold:** Pantone 1245 C / `#C08D23`
- **Inbound only:** the service never reaches out, never DMs, never pulls

## MCP tools

| Tool | Tier | Description |
|---|---|---|
| `log_ingest` | metered | Ingest NDJSON batch (max 4 MB / 10k lines). $0.0001/line + retention surcharge. |
| `log_tail` | 0 (free) | Return last N log lines for the calling DID. |
| `log_search` | 0 (free) | Search own-DID logs by time/severity/tag/free-text. |
| `log_retention_get` | 0 (free) | Return current tier + bytes stored. |
| `log_retention_set` | 0 (free) | Change retention tier (1d/7d/30d). |
| `log_today` | 0 (free) | Today aggregate — ingests, lines, bytes, charge_usd. |

## REST endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/log/ingest` | Ingest NDJSON batch. Returns 402 envelope first, accepts `tx_hash` to settle. |
| `GET` | `/v1/log/tail` | Last N lines for `did`. |
| `GET` | `/v1/log/search` | Search by time/severity/tag/`q`. Cursor pagination. |
| `GET` | `/v1/log/retention` | Current retention tier + monthly cost projection. |
| `PATCH` | `/v1/log/retention` | Change retention tier for a DID. |
| `GET` | `/v1/log/today` | Today aggregate (Tier 0, free). |
| `POST` | `/mcp` | MCP JSON-RPC 2.0 surface. |
| `GET` | `/health` | Service health. |

## Pricing

```
Ingest:    $0.0001 per log line
Retention:
  1d   (default)   free
  7d               $0.005 / MB
  30d              $0.02  / MB
Tail / search (own-DID):  Tier 0 — free
```

## NDJSON ingest example

```bash
curl -X POST https://hive-mcp-log.onrender.com/v1/log/ingest \
  -H 'content-type: application/json' \
  -H 'x-hive-did: did:hive:agent-foo' \
  -d '{
    "did": "did:hive:agent-foo",
    "retention_class": "7d",
    "lines": [
      { "ts": "2026-04-27T20:00:00Z", "severity": "info",  "tag": "tool", "msg": "tool_call ok" },
      { "ts": "2026-04-27T20:00:01Z", "severity": "error", "tag": "llm",  "msg": "rate_limited" }
    ]
  }'
```

The first call returns a `402` with the payment envelope (Base L2 USDC). Settle, then resend with `x-hive-tx: <hash>` for synchronous verification.

## Tail / search example

```bash
curl 'https://hive-mcp-log.onrender.com/v1/log/tail?did=did:hive:agent-foo&n=20'
curl 'https://hive-mcp-log.onrender.com/v1/log/search?did=did:hive:agent-foo&severity=error&q=rate'
```

## Retention

```bash
curl -X PATCH https://hive-mcp-log.onrender.com/v1/log/retention \
  -H 'content-type: application/json' \
  -d '{ "did": "did:hive:agent-foo", "tier": "7d" }'
```

A periodic sweep (default hourly) prunes lines older than each DID's retention window.

## Environment

| Var | Default | Notes |
|---|---|---|
| `PORT` | `3000` | |
| `ENABLE` | `true` | Flip to `false` to disable ingestion (returns 503 with `log_ingestion_disabled`). |
| `WALLET_ADDRESS` | `0x15184bf50b3d3f52b60434f8942b7d52f2eb436e` | W1 MONROE on Base L2. |
| `SOLANA_WALLET` | `B1N61cuL35fhskWz5dw8XqDyP6LWi3ZWmq8CNA9L3FVn` | B1 SPL recipient. |
| `USDC_BASE` | canonical | USDC contract on Base. |
| `BASE_RPC` | `https://mainnet.base.org` | |
| `PUBLIC_BASE_URL` | `https://hive-mcp-log.onrender.com` | |
| `MAX_LINES_PER_INGEST` | `10000` | |
| `MAX_BYTES_PER_INGEST` | `4194304` | 4 MB. |
| `RETENTION_SWEEP_MS` | `3600000` | 1 hour. |

## Security & scope

- Cross-DID reads are not exposed in v1 (own-DID only via tail/search).
- No PII detection in v1; client-side discipline assumed. Schema check is a v1.1 candidate.
- ZK cardinality attestation is a separate shim (`hive-mcp-zk-attestation`); not bundled.

## License

MIT — Steve Rotzin / Hive Civilization

<!-- HIVE-GAMIFICATION-META-START -->
## Hive Gamification

This MCP server is part of the Hive Civilization gamification surface (10-mechanic capability taxonomy).

- Capability taxonomy: https://hive-gamification.onrender.com/.well-known/hive-gamification.json
- Centrifuge dashboard: https://hive-gamification.onrender.com/.well-known/hive-centrifuge.json
- Consolidated OpenAPI: https://hive-gamification.onrender.com/.well-known/openapi.json

**Surface tags:** `gamification.spec.v1` · `gamification.surface.public` · `gamification.signal.read-only` · `gamification.settlement.real-rails`

Real rails on Base L2 (USDC `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913`). Read-only signal layer. Brand gold `#C08D23`.
<!-- HIVE-GAMIFICATION-META-END -->

## Hive Civilization Directory

Part of the Hive Civilization — agent-native financial infrastructure.

- Endpoint Directory: https://thehiveryiq.com
- Live Leaderboard: https://hive-a2amev.onrender.com/leaderboard
- Revenue Dashboard: https://hivemine-dashboard.onrender.com
- Other MCP Servers: https://github.com/srotzin?tab=repositories&q=hive-mcp

Brand: #C08D23
<!-- /hive-footer -->
