# Architecture

WeKeeper has two application layers over a shared local archive: dashboard/sync/private AI conversations, and the protocol group bot.

## Local archive and intake

`archive.py` owns transactional messages, source provenance, search, coverage and cached media metadata. Existing schema 4 and 5 stores retain their version on read or write; a new store uses schema 5. Read-only access never initializes or migrates a database. The retired UI bot's event production is removed; existing historical event tables are left intact.

The official-client collector and protocol snapshot bridge are alternative intake paths. The bridge stages a page durably, writes the retained snapshots through the public archive store, and then advances its own cursor. Its stable source identity, message row assignments and cursor survive restarts. It does not become a required gateway consumer, acknowledge bot events, or send messages. The gateway's control database remains owned by the gateway.

`core/` contains local database snapshots, decryption and process helpers. Cached media and document extraction remain local. Optional native voice conversion has its own explicit request and acceptance gate; removal of UI bot sending does not remove voice support.

## Dashboard and private questions

`dashboard/` contains outgoing sync, the local AI worker, independent SIWC authorization, cloud read models, security and usage. The browser is in `frontend/`; deployment assets are in `deploy/dashboard/`.

Sync sends only allowlisted groups' text projections and protected avatar thumbnails. Partition revisions, hashes, queued retries and idempotent server ingestion handle reconnects and lost acknowledgements. Collector health, sync health, authorization and quota retain separate meanings.

Private questions use leased cloud jobs and local scoped search, message context and statistics tools. Each turn freezes its selected groups and time range. The worker stores completed events before upload; restart recovery does not repeat uncertain inference. Conversations remain private to their creator.

Independent SIWC authorization, shared inference pause state and process locks are separate from all application task queues. Removing scheduled summaries does not remove login, refresh, `auth resume`, request concurrency limits, actual usage or the private question worker. There is no background summary scheduler or precomputed-summary tool.

## Protocol group bot

`bot/` contains:

- `protocol_gateway`, `protocol_inbox` and `protocol_native_inbox`: account-bound receipt, persisted intake, native source snapshots and ACK handling.
- `protocol_business`, `store`, `service` and `request_policy`: admission, business authorization, idempotent writes and recovery.
- `reply_generator`, `memory_host`, `persona` and `providers`: generation with request-scoped business tools and the selected local model credentials.
- `protocol_outbox`, `protocol_history` and `deferred_usage`: frozen reply delivery, confirmation, existing conversation history and durable usage completion.
- `protocol_runtime` and `control`: independent processing lanes plus opt-in outgoing cloud management.

One activated consumer retains its initial account, generation and receive boundary. An ordinary restart must not activate again. A request can produce at most one final reply. An uncertain send is reconciled using its existing identifier; recovery does not submit another send to obtain certainty.

Cloud management starts only after explicit alignment of the active local configuration. The control loop is independent of reception, generation, projection, sending and reconciliation. It retains group allowlists, pseudonymous member resolution, provider verification and monotonic configuration revisions. Protocol outbox acceptance is reported as gateway acceptance; it is not a phone-delivery assertion.

## Package and deployment boundaries

The import name remains `chatlog_keeper`. Three installed entrypoints are `chatlog-keeper`, `wekeeper-dashboard`, and `wekeeper-bot`. The archive MCP retains fifteen tools and no bot send permission; the separate business MCP retains its scoped permission model.

Wheels contain the Python runtime and required native-source helpers. Source distributions additionally contain frontend source/lockfiles, offline tests, documentation, portable unit templates and the complete pinned gateway patch chain. Rust build trees, account state, model keys, gateway tokens and generated reports are private deployment resources, not release contents.

Production services should use immutable, checksummed release directories. A source checkout can then be maintained without changing code underneath active processes. State paths and persisted queues stay independent of release paths.
