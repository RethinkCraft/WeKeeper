# Upstream and attribution

WeKeeper 0.5.0 is maintained as a Linux WeChat archive and MCP project derived
from [labazhou2024/chatlog-keeper](https://github.com/labazhou2024/chatlog-keeper).
The imported upstream baseline is commit
[`2225294fd0761e10ce15540f21e7dbb2ac304c3c`](https://github.com/labazhou2024/chatlog-keeper/commit/2225294fd0761e10ce15540f21e7dbb2ac304c3c)
(upstream 0.3.7rc0).

The original MIT license and copyright notice are retained in [LICENSE](LICENSE).
WeKeeper is published with an independent initial history containing the curated
current working tree; that choice does not remove upstream attribution. The
GitHub project name is WeKeeper, while the distribution/import/CLI identifiers
remain `chatlog-keeper`, `chatlog_keeper` and `chatlog-keeper` for compatibility.
The MCP identifier remains `wechat-archive`.

## Retained foundation

- Native Linux official-client/data discovery and account-bound key acquisition
  with database HMAC verification.
- WeChat database decryption, stable main/WAL snapshots, contact/group identity
  reading, bounded message reads and local image decoding.
- Private-file handling and low-level Linux process/memory helpers required for
  explicit setup.

## Linux-only scope

The curated current tree removes QQ and Windows/macOS support, their recovery/
binding/multi-source protocols, legacy export paths and frozen multi-platform
release packaging. Existing upstream commits are not imported into WeKeeper's
independent history; the pinned source reference above provides the baseline.
Linux CI and source/wheel build validation are added for this publication.

## Added archive and MCP behavior

- One transactional SQLite group collector with stable message references,
  provenance, resumable row-based increments, local history backfill,
  reconciliation, shard discovery/reset handling and retained raw payloads.
- Structured quotes, merged forwards, mentions, group events and observed member
  snapshots; scoped FTS5 search, context, deterministic statistics and coverage.
- Content-addressed cached media, quality distinctions, verified file association,
  bounded document extraction and actual MCP image content.
- Independent client/session/network/reception evidence and persisted local
  desktop alerts, rather than treating readable databases as proof of an online
  WeChat client.
- Original cached voice audio, versioned profiles/avatars, local cards and ordered
  note components, opt-in derived-text search, and a separately gated native
  conversion queue with current-cache proof and strict UI targeting.
- Fifteen stdio tools: fourteen archive queries and one explicit native voice
  request. The SDK remains optional; there is no HTTP service, model API,
  vector database or external business store.

Version 0.5.0 uses schema/parser 4. Its exact tested environment and incomplete
media/type coverage are documented in [validation](docs/validation.md). Native
conversion is not generally enabled merely because the tool is registered.

The repository does not include WeChat binaries, local environments, account
databases, keys, chat/media samples, private reports or deployment backups.
