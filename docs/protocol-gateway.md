# Native protocol gateway

The gateway is a separate pinned process. Python submits account-bound, message-ID-based quote replies through its loopback HTTP API. It also provides received snapshots to the archive bridge. An existing verified gateway can remain online while the Python applications are upgraded.

## Reproducible materials

[upstream.lock.json](../deploy/protocol-gateway/upstream.lock.json) records the upstream repository and commit, Rust toolchain, Cargo lock digest, and ordered patch checksums. The complete patch chain ships in the source distribution.

```bash
# Offline: checks the packaged lock and every patch digest.
python scripts/setup_protocol_gateway.py --verify-lock
# Prepares and builds the exact pinned source; does not log in or send.
python scripts/setup_protocol_gateway.py --build
# Verifies an existing prepared source tree without changing it.
python scripts/setup_protocol_gateway.py --check
```

Source verification checks all input blobs, modes and the complete ordered patch chain. Unlocked source changes are rejected. A successful build is not a live WeChat acceptance result.

## Ownership and credentials

Keep the gateway account profile, API token, journals and backups private. Bind HTTP to loopback. Exactly one process owns the account's sending profile. Preserve existing authenticated state during routine restarts; account logout and process restart are different operations.

Use rendered [gateway](../deploy/protocol-gateway/wekeeper-protocol-gate1.service.example) and [business](../deploy/protocol-gateway/wekeeper-protocol-business.service.example) templates. Set the exact account and own-user identities locally. Do not place their private values, tokens or host-specific paths in repository templates.

## Intake and delivery

A required consumer is first registered at a deliberately selected boundary. The Python inbox persists received pages and their native snapshots before ACK. A restarted consumer uses its existing checkpoint. Cursor expiry is an error requiring investigation, not permission to jump to the newest event.

The outbox freezes the account, thread, triggering source, reply body and persistent send identifier. It validates quote receipts and records uncertain outcomes for reconciliation. Existing accepted/unknown work is not resent to manufacture a new receipt. Gateway acceptance and phone-visible delivery remain distinct evidence.

## Archive bridge

The bridge reads retained events and snapshots without becoming a required consumer. It stages each page before writing and advances its persistent cursor only after successful archive writes. Both existing schema 4 and schema 5 archives use the same public storage implementation.

```bash
python scripts/run_archive_bridge.py \
  --archive-state "$ARCHIVE_STATE" --state-dir "$BRIDGE_STATE" \
  --journal "$BRIDGE_JOURNAL" --token-file "$GATEWAY_TOKEN" \
  --server http://127.0.0.1:6175 --account "$GATEWAY_ACCOUNT" \
  --bot-status "$BOT_STATUS" --catchup-floor "$INITIAL_CATCHUP_SINCE" --run
```

`--catchup-floor` is an explicit timezone-aware initial catch-up bound. Preserve the existing bound when upgrading; never substitute the current time. Keep the bridge database, source identity, row assignments, account mapping and existing cursor. Checking the command without `--run`/`--once` does not start copying.

The bridge does not inspect the live gateway control database. Optional session-renewal diagnostics have separate [service](../deploy/protocol-gateway/wekeeper-protocol-session-renew-verify.service.example) and [timer](../deploy/protocol-gateway/wekeeper-protocol-session-renew-verify.timer.example) templates and do not initiate login or sends.

## Upgrade and acceptance

Keep the gateway binary and its authenticated profile unchanged for a Python-only cleanup. Drain in-flight Python work, switch one worker at a time, retain checkpoints and verify receiver progress, sending and reconciliation. Use only an explicitly authorized test group for a new live mention and receiving-device verification. See [upgrading](upgrading.md).

Protocol diagnostics and benchmarks are development tools. Historical experiments are retained in Git history rather than being runtime dependencies or alternative production paths.
