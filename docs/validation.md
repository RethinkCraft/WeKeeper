# Validation and known limits

This document records the 0.5.0 baseline on **2026-10-26**. Counts describe a
specific verification snapshot, not advertised capacities or a guarantee that
another local account contains the same data. No private account IDs, group names,
message bodies, screenshots, media or acceptance reports are distributed here.

## Reproducible repository checks

```bash
python -m pip install -e '.[dev,mcp]'
python -m pytest -q
python -m chatlog_keeper.cli --help
```

The pre-publication package baseline passed **655 tests**. An additional **15
local deployment-harness tests** passed outside this repository; they are not
part of the public test suite or GitHub CI. Publication adds 23 mocked
accessibility-setup tests, bringing this initial source snapshot to **678 passing
tests**. The current CI result is authoritative for the published revision.

Repository tests use synthetic data and replace native UI primitives with
fakes. They do not send messages, convert real voices, log in, download media or
need access to an actual account. Coverage includes:

- Transaction interruption, duplicate collection, restart recovery, same-second
  and late messages, shard discovery/reset, raw unknown payload retention and
  stable references/checkpoints.
- Read/search/context/statistics, scoped pagination, derived search indexes,
  quote targets including missing originals, forwards, mentions and group events.
- Cached media identity, decoding/quality, preserved originals, first-frame
  animation output, corruption and offline reads.
- Document input/output limits, inert HTML, encoded text, CSV, PDF, DOCX and XLSX
  content, provenance, cursor changes and isolated worker failures.
- Native target uniqueness, cache-proof contracts, queue reuse, build/account
  verification, timeouts, parent exit, held output pipes and missing audio.
- MCP SDK discovery/annotations, stdio transport and actual image content using
  synthetic archives.

CI runs on Ubuntu 24.04 and Python 3.11 with tests and package-build verification.
That environment validates the library and mocked boundaries, not a real WeChat
desktop session.

## Real-client baseline

| Item | Verified baseline |
|---|---|
| Host | Ubuntu 26.04 x86_64 |
| Runtime | Python 3.11.15 |
| Client | Official Linux WeChat 4.1.13.26, reviewed executable build |
| Storage/parser | SQLite schema 4, parser 4 |
| MCP | All fifteen tools invoked from the actual consuming Codex conversation |
| Existing image | 1080×1920 full-cache pixels delivered as MCP image content; decoded content unchanged |
| Voice assets | 74 original cached SILK assets associated and read back |
| Avatar assets | 34 cached avatars associated and read back, including seven group avatars |
| Notes | 24 notes expanded into 387 ordered direct components |

The schema 3→4 upgrade generated a private consistent backup. Read-only comparison
confirmed preserved message references, first-seen times, origins/source
generations and non-regressing collection checkpoints. Historical reparsing used
batches of at most 500 and completed. New natural messages continued to arrive
with health reported as `receiving`; no test message or file was sent.

Text and image quote targets, actual image delivery, scoped reading/search,
context, statistics, forwards, member data, profile/avatar delivery, cards and
explicit missing-media states were exercised through MCP. These checks are
separate from a standalone stdio process passing its synthetic tests.

## Native voice acceptance

An existing 2.260-second voice completed exact native targeting, WeChat's own
conversion, accessibility-result capture, archive association and offline reading.
The complete automation also reused the existing native text after proving the
current authenticated `VoiceInfo` cache still matched the archived raw hash.
An 8.280-second voice request was queued through MCP, completed with the existing
client-native result, and returned the same stored result on a repeated request.

The accepted scope is **same-day, uniquely visible history** on the reviewed
build. Older dates, an unseen/ambiguous target, missing current audio, unavailable
keys, a changed build/UI or login loss stop the operation. The task has one active
worker and a 90-second deadline. A stored audio copy alone never authorizes an
operation that could require an uncached client download.

New installations and unverified builds remain disabled by default. No local or
third-party ASR, OCR, message/file sending or automatic login participated in the
acceptance. Accessibility restart preparation is not itself native acceptance.

## Media and semantic gaps

The 387 real note components comprise **150 text, 213 image and 24 HTML attachment
components**. All 237 media components remained unlinked: 213 reported an
unverified image-content association and 24 an unresolved file link. Consequently,
**zero real note images or HTML attachment bodies were accepted as readable**.
Complete ordered note text does not imply complete media access. The extractor's
synthetic HTML tests establish parser behavior only.

Forward/note media associations must have exact identity or content proof.
Bundled system emoji resources and similar filenames cannot substitute for a
message's cache. First-frame animation behavior is tested with synthetic assets;
real Linux sticker variants still require dedicated samples.

Real samples are also outstanding for location, namecard, ordinary webpage/
article, mini-program and video-channel variants. Those parsers have synthetic
regressions and preserve raw data, but are not advertised as comprehensively
Linux-validated. Naturally occurring ordinary file attachments/document bodies
also need broader real-client acceptance; format fixtures are synthetic.

Member directories describe observed local membership, not complete historical
membership or verified administrator lists. Quit/kick and other unobserved
system-event variants remain pending real samples. Source metadata that vanished
before collection cannot be reconstructed.

## Product boundaries

Only data actually synchronized to this device and still available when collected
can be preserved. Full remote history and uncached media cannot be recovered.
Full-cache image quality is distinguished from proven sender-original quality.
Quiet groups can lead to `sync_unverified`; the state is not a definite logout.

OCR, video processing, Moments reading, external page-body fetching, active media
downloads and a generic desktop automation API are outside this release. The
next work is prioritized in [ROADMAP.md](../ROADMAP.md).
