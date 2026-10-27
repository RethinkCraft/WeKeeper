# Changelog

## 0.6.0 — Unreleased

- Add an optional self-hosted FastAPI/SQLite and React dashboard with allowlisted
  group synchronization, scoped search/statistics, user roles, SSE refresh and
  persistent site alerts.
- Add private group-analysis conversations, local execution with durable result
  delivery, and bounded incremental summaries using independent ChatGPT authorization.
- Record actual per-request usage and show separately sourced official Codex quota
  with freshness and authorization status.
- Add real, stable-identity avatar thumbnails and opt-in missing-avatar backfill;
  default to seven automatically advancing Shanghai calendar days.
- Include deployment/service examples, source-package frontend assets and tests,
  and Python/frontend/package CI checks.

The dashboard exports selected text and avatar thumbnails to your cloud server;
AI workers send scoped analysis context to OpenAI. Credentials, source databases,
keys and ordinary chat media remain on the WeChat machine. Avatar backfill only
fetches supported saved HTTPS avatar URLs. See [dashboard operation](docs/dashboard.md).

Upgrade: archive schema/parser remain **4**, preserving the fifteen-tool MCP
interface. Existing installations can keep the base archive/MCP workflow. Dashboard
operators should back up cloud data and preserve local worker identity, queues and
authorization before updating; use the separate `[web]` extra and configured services.

## 0.5.0 — 2026-10-26

Initial WeKeeper Linux archive/MCP publication: resumable group collection, schema 4,
cached media and document reading, health evidence, and fifteen stdio MCP tools
including explicitly gated native voice conversion. Original MIT attribution is
retained; see [upstream provenance](UPSTREAM.md). Historical acceptance evidence and
remaining media/client gaps are preserved in [validation](docs/validation.md).
