# Validation and evidence boundaries

## Release checks

The 0.7.0 regression suite covers shared archival, schema 4/5 compatibility, message projection, media/document readers, permissions, synchronization, private questions and the protocol group bot. Default tests use synthetic data and mocked providers in temporary state directories. They must run without `private/`, an older checkout or machine-specific files.

Required checks are Python tests, frontend tests and production build, wheel/sdist content verification, the complete gateway patch checksum chain, source-package tests and frontend build, and installed-wheel CLI/MCP checks outside the checkout. The original fifteen-tool archive MCP contract remains: fourteen read-only queries and one gated native voice operation.

Archive compatibility tests assert that schema 4 stays 4 and schema 5 stays 5 through read/write/reopen, sync projections and avatar writes. Existing old bot-event records are retained; new intake does not produce them. Bridge tests cover staged pages, committed writes before checkpoint, retries, source identity, row assignment, floor persistence and unchanged cursors.

Question tests cover group/time scoping, evidence references, privacy, leases, cancellation, interrupted model execution and completed-result replay. SIWC tests cover durable shared pauses, explicit resume and safe migration before retired summary files are removed. Cloud migration tests assert retained data and deletion of summary-only records.

Protocol bot tests cover real-mention admission, account/group isolation, business permissions, idempotent reports, partial success, persona/configuration changes, memory MCP isolation, frozen replies, unknown sends and restart reconciliation. Cloud control uses protocol snapshots and explicit initial alignment rather than the old UI worker.

## 0.7.0 release verification — 2026-11-06

The clean source distribution passed **2160 Python tests** and **19 frontend tests**, with a successful production build. An independently installed wheel passed all three CLI entrypoints and the fifteen-tool MCP contract; all twenty locked gateway patches passed checksum verification.

Live checks confirmed schema 4 preservation, healthy synchronization with no pending partitions, cloud schema 3 removal of summary content, a completed private AI statistics question with the correct scoped count, and a cloud configuration save/application round trip that preserved the effective local model, persona and business settings. Final live acceptance passed on 2026-11-06: three fresh real mentions in the original bound test group each produced one accepted reply with matching source identities, one archived/cloud-synchronized message and one completed, synchronized usage record. The user confirmed a single corresponding phone-visible reply and working navigation to the quoted original. A normal business-worker restart preserved completed reply/send/usage records and consumer boundaries without duplicate sends; the gateway remained running. Recipient evidence is the explicit user confirmation in this chat, separate from machine-side gateway receipts.

## Live acceptance

A passing offline suite does not establish receiving-device delivery. Live acceptance must use an explicitly authorized existing test group and a newly received mention. Check the final message body, exact quoted original, quote navigation when available, and no duplicate reply. Gateway acceptance is tracked separately from that evidence.

During an upgrade, also check multiple sync/heartbeat cycles, a real private question, provider authorization/usage, cloud configuration acknowledgement and unchanged archive version. The protocol gateway must retain its authenticated profile and consumer boundaries throughout the Python cutover.

## Historical boundaries

The official-client archive reader baseline was verified on Ubuntu 26.04 x86_64, Python 3.11.15 and Linux WeChat 4.1.13.26. Other client builds require separate compatibility checks. Optional native voice conversion remains bounded to separately accepted client/layout conditions.

Cached file reading supports PDF, DOCX, XLSX, CSV/text and inert HTML. Cached-image identity, cache quality and explicit missing states remain part of results. Some note/forward media and Linux card variants still lack sufficient real-sample association evidence; synthetic format coverage is not proof of every real variant. There is no OCR, Moments reader, general media downloader or arbitrary desktop automation.

Historical UI-bot and performance experiments are available in Git history. They do not establish acceptance of this protocol-only release or remain part of its runtime.
