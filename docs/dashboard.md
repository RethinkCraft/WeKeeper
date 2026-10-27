# Self-hosted group dashboard

The optional dashboard keeps collection and ChatGPT authorization on the WeChat
machine. An outgoing sync service sends only explicitly selected groups' text
projections and cached avatar thumbnails to a separate cloud SQLite database. Existing MCP tools and archive
schema are unchanged. There is no incoming connection to the WeChat machine.

## Local setup

Install Python 3.11+ and the optional runtime:

```sh
python -m pip install -e '.[web]'
wekeeper-dashboard --archive-state /private/state groups
wekeeper-dashboard --archive-state /private/state configure \
  --server-url https://PUBLIC_IP:9443 \
  --group group_REFERENCE --token-file /protected/machine-token
```

`configure` requires explicit archive group references. The machine token must
match the cloud service. Keep config and the entire `STATE/dashboard` directory
private, outside source control. The directory stores the sync source identity,
sender pseudonym salt, queues, cached summaries and separate ChatGPT credentials.
It must survive restarts. Running services re-read the allowlist each cycle;
removed groups are no longer exported or analyzed. Existing cloud copies remain
available until an operator explicitly removes them; changing the allowlist does
not silently delete history.

Run `sync --once` to check the connection, then install the three service examples
under `deploy/` with paths adjusted to your checkout and archive state. Sync scans
every 30 seconds, with independent 30-second health heartbeats. Failed payloads
stay queued; other valid partitions continue. The cloud displays upload errors
separately from collector and heartbeat health.

## Independent ChatGPT authorization

WeKeeper uses the official **Sign in with ChatGPT** flow, not an API key or a
copied Codex login. The account must grant `chatgpt.tokens.use.direct` and permit
the selected model and serving region. This consumes the account's existing
ChatGPT plan allowance. Model inference still sends selected text to OpenAI.
Review app limits in [ChatGPT settings](https://chatgpt.com/settings/usage).

```sh
wekeeper-dashboard --archive-state /private/state auth login --port 1455 --no-browser
wekeeper-dashboard --archive-state /private/state auth models
wekeeper-dashboard --archive-state /private/state auth model MODEL_SLUG
wekeeper-dashboard --archive-state /private/state auth resume
```

Open the printed `http://127.0.0.1:1455/login` on the machine running the worker.
When using a browser on another computer, first forward that port over SSH:

```sh
ssh -N -L 1455:127.0.0.1:1455 WECHAT_MACHINE
```

Keep the forwarding connection open until authorization completes. The callback
listens only on loopback. Tokens, issued client registrations and host ID remain
in protected local storage; refresh-token rotation is serialized and atomic.
`auth status` shows only account labels and safe status. Use `auth login
--new-account` to add a separate registration, `auth account ID` to select one,
and `auth logout` to revoke the selected renewable session.

The summary scheduler checks its state every 30 seconds and normally processes
changed content every 30 minutes. Its first baseline includes today and yesterday
in Shanghai time; later corrections can affect any previously observed date.
Explicit `summarize --once --history` also establishes older baselines and consumes
additional allowance. Long days are processed in bounded incremental batches.
Only complete Responses streams and valid evidence references are saved. A
quota/permission pause requires `auth resume` after the issue is resolved. There
is no automatic switch to a paid API key. Summaries, statistics and syncing have
independent lifecycles.

### Incremental summaries and web conversations

The summary worker now tracks semantic message changes persistently, including
late arrivals, corrections and deletions on old dates. The default baseline only
analyzes today and yesterday; older history is not automatically sent to a model.
Each batch contains at most 500 messages or 20,000 characters. Oversized messages
are segmented with their original evidence reference; pending batches remain in
the queue. Up to 50 neighboring messages and bounded structured context provide
background. A failed or invalid result never advances the checkpoint. The same
input has a bounded retry budget, and daily summaries identify incomplete coverage.
Coverage/health metadata and group display-name changes do not cause inference.

Install `deploy/wekeeper-agent.service.example` for the outgoing web-chat worker.
Signed-in viewers and administrators can create private group-analysis conversations
and continue them from another browser. Every turn freezes its selected groups and
time range. The local worker offers only scoped message search, context, statistics,
and summary tools; it has no shell or arbitrary file execution tools. Credentials
stay on the WeChat machine. Responses use the same independent SIWC authorization
as automatic summaries. The inference lock serializes model requests and summaries
yield between batches when a chat is waiting.

Cloud tasks use leases and sequenced, idempotent event uploads. A worker restart
marks uncertain executions interrupted instead of automatically repeating inference.
Completed results are persisted locally before delivery, so a lost upload response
can be replayed without another model request. Disable/allowlist changes apply to
new work and local tool reads; conversations remain private to their creator.

### Usage and official Codex quota

Every new summary/chat run records actual provider token usage, including known
usage on output-validation failures. Missing usage is unknown, never zero. Old
requests without records cannot be reconstructed. Administrators see all application
usage; viewers see their own chat usage.

The sync service separately reads `account/rateLimits/read` from a local Codex
app-server using its existing login. It never starts a Codex model turn, copies
Codex tokens, or publishes an app-server listener. Only allowlisted quota fields
are uploaded. Official windows, reset timestamps and sampling time appear for
administrators, with data older than 90 seconds marked stale. This Codex login and
WeKeeper's independent SIWC grant are **not verified to be the same account or
metered allowance**. Their quota, application consumption and authorization status
are displayed separately. A SIWC limit can therefore pause WeKeeper even if the
Codex quota card shows remaining allowance.

The default browser range is Shanghai today plus the prior six calendar days,
ending now. Relative presets advance automatically; custom historical dates stay
fixed. Choices are saved per signed-in user and view in the current browser.
Message pagination freezes its query boundaries until “view latest” is selected.

Only cached member avatar thumbnails (re-encoded JPEG, at most 128 px) are added
to cloud sync. Matching uses exact native identities locally and stable keyed
pseudonyms in the cloud. Missing avatars retain stable placeholders. Ordinary
chat images, voice files and attachments remain local. Cloud avatars require a
valid dashboard login and observed membership in the requested group.

### Automatically fill missing member avatars

The optional `avatar-backfill` worker reads saved `small_head_url` and
`big_head_url` values from the selected account's contact database. It only
fetches missing avatars for members of the dashboard's selected groups, using
exact member identifiers. The first supported source is HTTPS on `wx.qlogo.cn`.
It validates image contents, dimensions and size, re-encodes the image, and
archives it locally before the ordinary thumbnail sync uploads it.

```sh
wekeeper-dashboard --archive-state /private/state avatar-backfill --once
```

Install `deploy/wekeeper-avatar-backfill.service.example` for continuous work.
The worker has a private durable queue, bounded retries and download limits, and
does not block message collection or send requests to a model. Raw image URLs
remain local. Existing good avatars are reused. Members who have not spoken can
be prepared locally; their cloud avatar appears after an observed message is
synced. Missing or expired source URLs remain unavailable rather than displaying
another person's image. This does not establish that the local roster includes
every current or historical group member.

Reference: [registration](https://developers.openai.com/siwc/token-sharing-open-source/sign-in),
[inference](https://developers.openai.com/siwc/token-sharing-open-source/models-and-inference),
[error recovery](https://developers.openai.com/siwc/token-sharing-open-source/errors-and-recovery).

## Cloud deployment

`deploy/dashboard/Dockerfile` builds the React/TypeScript frontend and the Python
service. Copy only source and deployment files to the cloud; never copy the
WeChat state directory, OAuth credentials, media or this repository's `private/`.

Create `deploy/dashboard/.env` with mode `0600`:

```dotenv
WEKEEPER_MACHINE_TOKEN=<independently-generated-machine-token>
WEKEEPER_PUBLIC_ORIGIN=https://PUBLIC_IP:9443
```

Provision `runtime/data` and `runtime/backups` with owner UID/GID `10001:10001`.
For a new server, use the supplied bootstrap HTTP configuration and Certbot
webroot challenge to obtain a publicly trusted IP certificate, then render
`nginx.conf.template`, validate Nginx and enable TLS. Certbot 5.4+ supports
`--preferred-profile shortlived --webroot --webroot-path /var/www/acme
--ip-address PUBLIC_IP`. Validate staging issuance before production issuance.
Port 80 must reach the challenge webroot. IP certificates last about six days;
enable the supplied renewal timer and test `renew --dry-run`.

If host Nginx already serves other applications, preserve it and start only API:

```sh
docker compose -f compose.yaml -f compose.host-nginx.yaml up -d --build api
```

Render `nginx.host.conf.template` for a free TLS port (for example 9443), using
the existing trusted IP certificate and its renewal mechanism when available.
The API listens only through `127.0.0.1:18880`. Validate `nginx -t` before reload.
Allow the selected TLS port in the cloud security group. Never replace an
existing site's configuration merely to claim ports 80/443.

An existing HTTPS site can alternatively include `nginx.subpath.conf` to serve
the dashboard under `/wekeeper/`. Set `WEKEEPER_BASE_PATH=/wekeeper/` for a source
build, use `https://PUBLIC_IP/wekeeper` as the local sync `server_url`, and keep
`WEKEEPER_PUBLIC_ORIGIN=https://PUBLIC_IP` (an origin has no path). For an already
verified frontend bundle, `compose.prebuilt.yaml` uses `Dockerfile.prebuilt`;
build that bundle with `npm run build -- --base=/wekeeper/` first.

If Docker's containerd image store reports a missing parent snapshot while
unpacking a cached build, export just this project's image with
`docker buildx build --provenance=false --output type=docker,dest=/tmp/wekeeper-image.tar
-t wekeeper-api:latest -f deploy/dashboard/Dockerfile.prebuilt .`, load it with
`docker load -i /tmp/wekeeper-image.tar`, then start API with `--no-build`.
This avoids pruning images or restarting unrelated containers.

Bootstrap the first administrator interactively, or pipe a protected password
file to `user-add chris --role admin --password-stdin` inside the API container.
Passwords require at least 12 characters. There is no public registration.
Administrators create/disable accounts and maintain literal keyword rules;
viewers can read selected groups and mark their own alerts read/acknowledged.

## Data, alerts and recovery

Machine routes are `/v1/ingest/partitions`, `/v1/ingest/heartbeat`, and
`/v1/ingest/summaries`, all protected by the machine bearer token. Browser routes
use independent, revocable server sessions, secure HttpOnly cookies, same-origin
checks and CSRF tokens. The UI uses one SSE connection carrying revision-based
refresh notifications; all displayed content is fetched through authorized APIs.

Partition hashes cover canonical JSON and revisions make replay idempotent.
Atomic replacement covers late messages, deletions, deduplication and text
enrichment without treating archive query cursors as replication watermarks.
Message, card and transcript text remain one top-level message. Sender IDs are
stable keyed pseudonyms; cross-day active counts use distinct IDs. Source
database IDs, raw payloads, file paths and binary media are not exported.

Keyword rules match future top-level messages with literal case-insensitive
text matching. Initial history does not trigger alerts; replay does not duplicate
them. Health rules distinguish session presence, actual recent reception,
collector faults, upload failures and source heartbeats. Uncertain observations
do not prove recovery. User acknowledgment does not resolve an active fault.

Use `backup.sh` and the supplied daily timer for SQLite online backups with an
integrity check. Backups contain private text and account password hashes; protect
and copy them off the cloud server. To restore, stop only the API, restore a
verified backup into its data directory with UID/GID 10001, and restart the API.
Heartbeat manifests reconcile missing partitions and completed summaries with
the local durable queues; summaries can be resent without another model request.
Preserve the local state identity and queues when restoring the WeChat machine.

Tests use synthetic archives and fake model HTTP responses. Passing them does
not establish real account eligibility or actual model-output quality; validate
one explicitly authorized live summary after local sign-in.

For a remote owner without a local SSH forwarding session,
`scripts/remote_summary_auth.py` provides a short-lived HTTPS relay page through
an operator-created private reverse tunnel. Its unpredictable URL must have
access logging disabled; it requires an exact trusted origin and CSRF token and
only forwards the fixed loopback callback. The owner copies the failed loopback
redirect into that page, never into chat. Tokens are exchanged and stored by the
local worker only. Remove the temporary Nginx route and tunnel immediately after
authorization or expiry. This helper is not a permanent dashboard endpoint.
