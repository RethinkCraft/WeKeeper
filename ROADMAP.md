# Roadmap

WeKeeper 0.5.0 provides local Linux group archival and a fifteen-tool stdio MCP
surface. Scope follows one collector, one SQLite archive and one MCP layer.
This roadmap distinguishes implemented behavior from real-client acceptance;
details and the dated baseline are in [validation](docs/validation.md).

## Implemented and real-client verified

- Continuous archival of discovered groups, new shards, resumable local history
  backfill and restart catch-up, with raw provenance and stable message identity.
- Scoped reading, literal search, context, quoted text/image relationships,
  deterministic counts and explicit archive coverage.
- Actual cached image pixels over MCP, source/quality metadata and offline copies.
- Merged-forward expansion, source-backed mentions, observed member names/owner
  and sampled join/rename/revoke events.
- Raw cached SILK audio and versioned cached avatars with exact identity mapping.
- Ordered local note text/components without inflating group-message statistics.
- Native voice conversion and existing-text reuse on the reviewed Linux client,
  limited to same-day uniquely visible targets with current-cache proof; disabled
  by default for unverified installations/builds.
- Client health and local alerts separating recent message reception, session
  presence, definite failures and unverified silence.

Schema 4 migration preserves references, first-seen times, source generations and
checkpoints, and creates a private backup. Fourteen tools remain read-only; only
explicit accepted voice requests may operate the client.

## Implemented with real-sample gaps

| Capability | Remaining acceptance work |
|---|---|
| Note/forward media | Prove cache associations; all 213 real note images and 24 HTML attachments in the baseline remain unlinked |
| File/document reading | Broader naturally occurring Linux file samples; PDF, DOCX, XLSX, CSV/text and inert HTML fixtures currently establish format behavior |
| Stickers/animation | Real message-cache variants; synthetic tests already preserve originals and identify a static first frame |
| Location/namecard/link/article/mini-program/channel cards | Confirm real Linux variant fields and associations; local parsers are synthetic-tested |
| Additional system events | Real quit/kick and other missing templates; retain raw payloads rather than inventing identity |

## Next priorities

1. Establish reliable cached note/forward media linkage using identifiers or
   content evidence; preserve explicit missing states until proven.
2. Expand native voice targeting beyond today's visible history through separate
   real acceptance for each date/navigation layout. Preserve single-task limits,
   source-cache proof and stop-on-ambiguity behavior.
3. Add naturally observed document/card/sticker cases as synthetic, deidentified
   regression fixtures, without sending test messages or files.
4. Improve portability diagnostics and document compatibility per reviewed client
   build rather than promising all Linux/WeChat versions.

## Deferred

OCR and cross-media text indexing, video processing, external page-body fetching,
Moments, uncached-media downloads and general-purpose desktop automation remain
separate future work. No ASR model/service is introduced for the native-voice path.

Every development and acceptance pass must avoid sending WeChat messages or
files, creating sample interactions or automatically logging in. Ordinary
queries stay archive-only, and chat/document instructions remain untrusted data.
