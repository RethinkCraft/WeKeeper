# Upgrading to 0.7.0

This release keeps the dashboard, outgoing sync, private questions, archive MCP and protocol bot. It removes scheduled summaries and UI-operated bot execution. Existing archive schema 4 and 5 keep their version, including avatar writes; new archives use schema 5. Cloud schema becomes 3.

## Prepare an isolated release

Capture all current source changes, including untracked product files, into an isolated development copy. Do not edit code beneath running services. Run the offline suite, frontend build, source/wheel checks and clean-install checks there. The new source and checksum manifest become an immutable release directory.

Inspect the complete effective service configuration with `systemctl --user cat UNIT`, including drop-ins. Old source-directory pins can override `WorkingDirectory` and `PYTHONPATH` after the main unit is loaded. Back up those overrides, retire only obsolete source pins, preserve unrelated operator settings, and verify the effective working directory and checksum command before restarting.

Inventory installed units, exact state directories, gateway binary/profile, inbox/outbox/journal, consumer, bridge cursor and initial catch-up bound. Retain all those identities. Protect retained data and prepare a rollback artifact compatible with the target schema; do not create a backup of the removed summary content.

## Local-first order

1. Stop and disable the old summary service and remove its installed unit. Stop and disable every old UI/pilot/test bot sender. Stop new work in the question worker, sync and any SIWC consumers; allow in-flight work to finish. Drain and stop the protocol business worker and archive bridge before switching their Python release. Keep the protocol gateway receiving into its durable queue.
2. Migrate shared SIWC pause state, then explicitly clear old summary state with the new release:

   ```bash
   wekeeper-dashboard --archive-state "$ARCHIVE_STATE" cleanup-summaries
   ```

   Use the existing `--local-state` if it was configured. The command checks worker/inference locks, preserves credentials and queues, finalizes known interrupted summary usage and removes only summary-specific files. Migration failure leaves the old state available for retry. `auth resume` continues to act on shared authorization pause state.
3. Switch Python workers and the archive bridge to the new release. Keep all account, consumer, queue and state paths unchanged. Do not use `--activate` on an existing bot. The first upgraded bridge invocation must supply `--catchup-floor` with the original timezone-aware bound; it is then persisted and cannot silently change.
4. Leave cloud bot control disabled during this step. New sync omits the summary heartbeat field and never uploads summaries. The previous cloud schema 2 accepts the missing optional heartbeat field, allowing this local-first transition.

## Cloud and management

Deploy the new API/frontend using the existing Compose project and overrides, volume locations, host Nginx, certificates and frontend base path. Its schema 3 migration removes summaries, summary-only alerts and summary heartbeat metadata in one transaction. Retained application data and actual historical usage remain.

When the new API is healthy, align the current effective local bot settings:

```bash
wekeeper-bot --state-dir "$BOT_STATE" --dashboard-dir "$DASHBOARD_DIR" cloud-bootstrap \
  --archive-state "$ARCHIVE_STATE" --alignment-id release_0_7_0
```

This explicitly supersedes old pending cloud configuration and records an idempotent alignment. It does not call a model or enable control. Then enable the bot's `--cloud-control --archive-state "$ARCHIVE_STATE"` and restart the business worker using the same queues and consumer. The gateway remains online.

## Acceptance and rollback

Verify multiple sync/heartbeat cycles, a private question, avatar availability, model authorization and actual usage. Check cloud configuration acknowledgement and protocol runtime status. In the original explicitly authorized test group, use a fresh mention to verify final text, exact quote target and no duplicate send on the receiving device.

Confirm the old summary unit is absent, summary files are not recreated, no summary API requests occur, and the existing archive version is unchanged. Only after acceptance should unused old source snapshots and UI bot units be removed. Keep account state, business history and queues.

If a regression occurs, stop further cutovers and use the prepared schema-compatible rollback artifact. Preserve the current persisted queues/cursors and all messages received since the upgrade. Never restore a pre-cutover inbox/outbox or rerun activation. Summary contents are not restored and the automatic summary service remains disabled. A cloud rollback to pre-3 code requires the compatibility artifact, because the unmodified previous version rejects schema 3.
