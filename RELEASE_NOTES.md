# Release Notes

## v1.0.0 — 2026-04-27

Initial public release.

- MCP 2024-11-05 over Streamable-HTTP / JSON-RPC 2.0 at `POST /mcp`
- 6 MCP tools: `log_ingest`, `log_tail`, `log_search`, `log_retention_get`, `log_retention_set`, `log_today`
- 7 REST endpoints under `/v1/log/*` plus `/mcp`, `/health`, `/.well-known/mcp.json`, `/`
- Pricing: $0.0001/line ingest; 1d retention free, 7d $0.005/MB, 30d $0.02/MB
- USDC settlement on Base L2 (W1 wallet) with synchronous tx verification
- Solana SPL USDC also accepted (B1 wallet)
- SQLite ledger at `/tmp/log.db`; periodic retention sweep prunes lines past each DID's tier window
- Inbound only — no outbound calls, no DMs, no spam
- `ENABLE=true` default — service ships live
- Hive Civilization brand gold `#C08D23` (Pantone 1245 C)
- Author: Steve Rotzin <steve@thehiveryiq.com>
