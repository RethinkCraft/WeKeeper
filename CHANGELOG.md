# Changelog

## 0.7.0 — 2026-11-06

- Retain dashboard/message synchronization/private questions and a single protocol group-bot runtime.
- Remove scheduled group summaries, their dedicated storage/APIs/UI, and the former UI bot/pilot/experimental harnesses.
- Separate SIWC pause/resume state from removed summary storage; retain authorization, scoped questions and real historical usage.
- Preserve existing archive schema 4/5 on reads and writes; make protocol archive bridging self-contained and persist its explicit initial catch-up bound.
- Add explicit local-first cloud bot alignment and an independent protocol control loop.
- Unify bot check/run entrypoints, portable service templates, complete gateway release materials and clean-install CI.
- Update English/Chinese operation, upgrade and architecture documentation.

Upgrade local producers before cloud schema 3, preserve all existing queues/consumer identities, and never reactivate a deployed consumer. Retired summary content is deleted without a new backup. See [upgrading](docs/upgrading.md).

## 0.6.0 — Development baseline

- Add a separate business MCP and group-bot worker for permission-scoped daily
  offer reporting/querying, durable reply tasks and versioned character presets.
- Reuse existing Sign in with ChatGPT authorization for GPT; support local
  DeepSeek/custom HTTPS API credentials, business-only tools and separate bot usage.
- Add dashboard business/persona/model controls with local acknowledgements,
  anonymous member references, optimistic state revisions and auditable changes.
- Migrate to schema 5 live candidates, prioritize realtime text collection and
  move bot-mode media/P2 work to an independent enrichment worker. Native quote
  sending remains gated on real-client acceptance.

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

The development branch introduced schema 5 for the former UI bot while some deployments retained schema 4. Version 0.7.0 supports both without an implicit 4→5 upgrade. The fifteen-tool archive MCP interface remains stable.

## 0.5.0 — 2026-10-26

Initial WeKeeper Linux archive/MCP publication: resumable group collection, schema 4,
cached media and document reading, health evidence, and fifteen stdio MCP tools
including explicitly gated native voice conversion. Original MIT attribution is
retained; see [upstream provenance](UPSTREAM.md). Historical acceptance evidence and
remaining media/client gaps are preserved in [validation](docs/validation.md).
