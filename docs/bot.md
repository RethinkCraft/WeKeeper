# Protocol group bot

The bot receives real mentions through the native protocol gateway, executes scoped business tools, and quotes the triggering message in its final reply. The former UI-operated worker, native quote UI adapter and pilot mirror are no longer supported.

## Install and configure

```bash
python -m pip install -e '.[bot,web]'
wekeeper-bot --state-dir "$BOT_STATE" bind --bot-wxid "$BOT_WXID"
wekeeper-bot --state-dir "$BOT_STATE" configure /protected/bot-config.json
wekeeper-bot --state-dir "$BOT_STATE" secret-set provider-main
```

A configuration defines groups, businesses, authorized reporters, offer items, personas and provider profiles. A credential reference points to a protected local key. SIWC uses the explicitly selected independent ChatGPT grant in `--dashboard-dir`; DeepSeek and compatible providers use their own local API credentials. No credentials are uploaded to the dashboard.

`verify-provider PROFILE_ID` is an explicit real model request that checks tool support and records usage. Ordinary configuration checks do not perform that request. Personas have one Markdown content source, with versioning; old stored personas and confirmed conversation history remain readable.

## Check, activate and run

The following variables denote existing protected files and exact verified identities. `BINDINGS` maps native gateway thread IDs to configured group references.

```bash
wekeeper-bot --state-dir "$BOT_STATE" --dashboard-dir "$DASHBOARD_DIR" --usage-dir "$USAGE_DIR" check \
  --inbox "$INBOX" --outbox "$OUTBOX" --journal "$JOURNAL" \
  --token-file "$GATEWAY_TOKEN" --bindings "$BINDINGS" \
  --server http://127.0.0.1:6175 --account "$GATEWAY_ACCOUNT" \
  --own "$BOT_WXID" --consumer "$CONSUMER" --status-file "$BOT_STATUS"
```

`check` is read-only. To start processing, use the same arguments with `run` instead of `check`. On a new deployment only, add `--activate` once after verifying the account and bindings. On restart, omit `--activate` and keep all inbox, outbox, journal, identity and consumer values unchanged.

`run` performs real model requests, authorized business actions and message sending. The old UI flags such as `--send` and `--native-lock-dir` are rejected. `scripts/run_protocol_bot.py` remains a compatibility wrapper: its default performs `check`, and its explicit `--run` starts the same protocol implementation.

Install a rendered copy of [the service template](../deploy/protocol-gateway/wekeeper-protocol-business.service.example). Keep one worker per state and consumer. Do not start an official-client UI sender alongside the protocol sender for the same account.

## Cloud management

The initial cloud alignment deliberately uses the currently effective local configuration, including its model, persona and business settings. Existing pending cloud commands are superseded. Alignment is idempotent and does not invoke a model or enable the control connection:

```bash
wekeeper-bot --state-dir "$BOT_STATE" --dashboard-dir "$DASHBOARD_DIR" cloud-bootstrap \
  --archive-state "$ARCHIVE_STATE" --alignment-id release_0_7_0
```

After the upgraded cloud API is healthy and alignment succeeds, add `--cloud-control --archive-state "$ARCHIVE_STATE"` to `run`. The local bootstrap marker is required. Cloud management can enable or edit only groups already present in the explicit native bindings file. Adding a new native binding is a local operation followed by a worker restart with the existing consumer/checkpoint; it never requires reactivation. The worker then acknowledges new cloud changes and uploads status. A disabled group or changed configuration cancels unsent work; new requests use the newly applied persona version.

Cloud management errors are recorded independently and do not stop the protocol processing lanes. The page reports the active model/persona, configured providers, queue state and gateway acceptance. Phone receipt must be verified on the receiving device.

## Business and recovery rules

Status is scoped to business, item and Shanghai calendar day: `unknown`, `available`, or `unavailable`. A new day does not copy yesterday's status. Only an authorized reporter's current explicit statement permits a write. Questions, quotes and nearby history cannot authorize one. Tool receipts determine what the bot may claim to have changed.

The model cannot choose a different quote target, run shell/SQL, or browse arbitrary URLs. There is at most one final reply per request. Intake is durable before ACK; business changes and delivery have idempotent identifiers. Recovery uses committed receipts and existing send identifiers. Unknown delivery does not trigger an automatic resend.

The separate external business MCP remains read-only:

```bash
wekeeper-bot --state-dir "$BOT_STATE" mcp --group-ref group_REFERENCE
```

## Verification

Default tests use synthetic accounts, messages and mocked providers. They cover permissions, configuration changes, business partial success, request isolation, lost ACKs and restart recovery. Live acceptance requires an explicitly authorized existing test group: trigger a new mention, confirm the final body, original quote target and absence of duplicate replies on the receiving device.

See [protocol gateway operation](protocol-gateway.md), [architecture](architecture.md), and [upgrading](upgrading.md).
