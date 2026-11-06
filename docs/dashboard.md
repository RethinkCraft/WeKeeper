# Self-hosted dashboard

The cloud dashboard provides selected-group messages, statistics, alerts, private AI conversations, bot management and usage. Local workers make outgoing authenticated connections; the cloud does not open a listener on the WeChat machine.

## Local configuration

```bash
python -m pip install -e '.[web]'
wekeeper-dashboard --archive-state "$ARCHIVE_STATE" groups
wekeeper-dashboard --archive-state "$ARCHIVE_STATE" configure \
  --server-url https://YOUR_SERVER/wekeeper \
  --group group_REFERENCE --token-file /protected/machine-token
wekeeper-dashboard --archive-state "$ARCHIVE_STATE" sync --once
```

Use explicit known group references. The private dashboard directory defaults to `ARCHIVE_STATE/dashboard`; `--local-state` can select another existing directory. Preserve its source identity, sender salt, sync queues, question queue, usage and SIWC credentials across upgrades. Changing the allowlist stops new exports and analysis for removed groups; existing cloud history requires an explicit separate removal.

| Local service template | Purpose |
|---|---|
| `deploy/wekeeper-sync.service.example` | Text, statistics, health, avatar thumbnails and usage sync |
| `deploy/wekeeper-agent.service.example` | Private browser questions |
| `deploy/wekeeper-avatar-backfill.service.example` | Optional missing-avatar retrieval |

Render every placeholder to an absolute path before installing a unit. Keep local proxy settings in a protected environment file outside version control. Do not install the removed summary worker. New installations create no summary database.

Message scans and independent health heartbeats normally run every 30 seconds. Failed partitions stay queued while other valid partitions continue. Archive collection or the protocol bridge remains a separate service.

## Independent ChatGPT authorization

The local question worker uses independent Sign in with ChatGPT authorization. Dashboard user login, the bot's API credentials, and a separate Codex login are distinct.

```bash
wekeeper-dashboard --archive-state "$ARCHIVE_STATE" auth login --port 1455 --no-browser
wekeeper-dashboard --archive-state "$ARCHIVE_STATE" auth models
wekeeper-dashboard --archive-state "$ARCHIVE_STATE" auth model MODEL_SLUG
wekeeper-dashboard --archive-state "$ARCHIVE_STATE" auth status
wekeeper-dashboard --archive-state "$ARCHIVE_STATE" auth resume
```

Complete login on the machine hosting the worker, or use an SSH loopback port forward to it. Tokens remain local and refresh rotation is serialized. A permission/quota pause remains durable until explicitly resumed after the underlying issue is resolved. There is no automatic fallback to a paid API key.

`scripts/remote_auth.py` is an optional short-lived OAuth callback relay; it does not perform inference and is not an always-on worker. Prefer the loopback forwarding method for new setups.

## Private questions and bot control

Signed-in users can create private analysis conversations and continue them from another browser. Each turn fixes its group/time scope. The local worker exposes only message search, context and computed statistics; users can explicitly ask it to summarize those results. There is no automatic summary schedule, precomputed-summary tool or summary page.

Tasks use leases and sequenced idempotent events. Completed results are persisted before upload. A restart reports uncertain execution as interrupted rather than repeating inference. User isolation, allowlist changes, cancellation and result replay retain their existing behavior.

The bot management page uses a separate protocol control connection. First align the currently active local bot configuration, then enable control as described in [bot operation](bot.md). Configurations are pending until acknowledged by the local worker. The dashboard does not store model keys. Status distinguishes gateway acceptance from receiving-device verification.

## Usage, quota and avatars

Actual provider usage remains recorded for private questions and bot requests, including known consumption on failed output validation. Historical summary consumption remains in the ledger; it contains no summary body. Missing usage is unknown rather than zero.

A separate local Codex app-server quota reader samples its official quota without starting a model turn or uploading its login tokens. Application usage and that quota retain distinct account/source/freshness labels; they are not assumed to represent the same allowance.

Only cached, re-encoded avatar thumbnails are synced, with stable keyed member pseudonyms. Ordinary chat images, voice and attachments remain local. Avatar access requires dashboard login and observed group membership.

Optional `avatar-backfill --once` fetches missing selected-group avatars from saved validated HTTPS contact URLs, archives them locally and leaves upload to ordinary sync. It uses exact member identities and bounded retries. It does not open ordinary chat media or call a model.

## Cloud deployment

The cloud service is FastAPI/SQLite with a React frontend. Source and frontend assets are public deployment material; account state, OAuth credentials and repository `private/` are not.

Create a protected `deploy/dashboard/.env` using these placeholders:

```dotenv
WEKEEPER_MACHINE_TOKEN=<independently-generated-token>
WEKEEPER_PUBLIC_ORIGIN=https://YOUR_SERVER
WEKEEPER_BASE_PATH=/wekeeper/
```

Use matching origins and paths throughout the proxy and frontend build. Provision `runtime/data` and `runtime/backups` for UID/GID `10001:10001`.

For a standalone Nginx/TLS stack, render the provided Nginx templates and configure certificates before starting:

```bash
cd deploy/dashboard
docker compose up -d --build api nginx
```

If the host already runs Nginx, preserve it and start only the API using the host override:

```bash
cd deploy/dashboard
docker compose -f compose.yaml -f compose.host-nginx.yaml up -d --build api
```

That API is exposed through `127.0.0.1:18880`. Include the supplied subpath configuration in the existing HTTPS server block for `/wekeeper/`, or render the host template for a separate TLS listener. Validate `nginx -t` before any reload.

For a prebuilt frontend, build with the deployment base path and add `-f compose.prebuilt.yaml` to the same Compose invocation:

```bash
(cd frontend && npm ci && npm test && npm run build -- --base=/wekeeper/)
```

`Dockerfile.prebuilt` uses that verified `frontend/dist`. The normal Dockerfile builds the frontend itself. Both run the API as the non-root application user with a persistent `/data` mount. Preserve the same Compose project, override files, volume paths and public URL during an upgrade.

Certificate provisioning/renewal templates remain under `deploy/dashboard/`; use the existing certificate mechanism when sharing a host proxy. Do not replace another site's configuration to acquire its ports.

## Data, alerts and recovery

Cloud schema 3 removes automatic summary content and its exclusive alerts. Existing messages, users, sessions, private conversations, bot configuration and real usage remain. The migration is transactional and repeatable. Upgrade local producers first so they no longer send summary fields or call retired summary APIs.

Retain the local source identity and queues when rebuilding the cloud service. Ordinary missing-ACK retries are idempotent. A restored cloud message database can be repaired through the sync reconciliation mechanism; restoring old bot consumer cursors is not a delivery-recovery strategy.

For an already upgraded cloud database, the CLI can create a consistent backup:

```bash
wekeeper-dashboard backup /protected/backups/dashboard.sqlite --data-dir /protected/cloud-data
```

Treat backups as private. Upgrade preparation preserves the retained application data without creating a new backup of retired summary contents. See [the upgrade procedure](upgrading.md).
