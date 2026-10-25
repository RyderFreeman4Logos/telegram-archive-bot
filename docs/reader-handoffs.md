# Daily corpus and external reader handoffs

**Spec-ID: DA09** — [#76](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/76).

The collector derives immutable account/day and combined day manifests from the
existing SQLite account, activity, observation, view and source revision records.
It uses the existing Notion document/outbox format. Local capture, verified remote
publication, reading and analysis have separate states. No model pipeline runs
inside the manifest worker.

## Configuration and operation

Normal builds include the `ntn` feature, and Notion publication defaults to
`enabled = true`. An unconfigured tool/token/destination remains
`pending_configuration`; it does not block local capture or manifest generation.
`notion.enabled = false` suppresses the transport, while a
`--no-default-features` build reports `compiled_out`. See
[Notion configuration](configuration.md#notion-publication-prerequisites-da05--72)
and [publication/reconciliation](notion.md#automatic-publication-and-recovery).
An SOP page is a reading specification, not an inferred archive destination.

```toml
[reader]
enabled = true
max_activities = 10000 # per account/day; an excess is a visible build error
max_windows = 1000    # accounts/windows per combined day
export_bytes = 100000000

[reader.sop]
id = "x-daily-briefing-v1"
version = "1.0.0"
# Synthetic example only; configure the deployment's actual authoritative page.
# page_id = "22222222-2222-2222-2222-222222222222"
# page_url = "https://www.notion.so/22222222222222222222222222222222"
```

`reader.enabled` defaults true. `run` starts its separate local worker even when
Telegram, model, translation, synthesis and Notion are disabled. It takes one
bounded turn each second. `SIGHUP` uses the existing atomic ConfigManager: new
turns use the accepted snapshot; invalid settings leave the previous snapshot
active. Config-off does not delete history, and explicit export remains available.
An SOP with neither a validated page ID nor URL makes `sop_configured = false`;
completed reader layers are
refused until a handoff pins the configured SOP. Changing the SOP identity/version
or URL creates new affected manifest revisions; it never mutates old handoffs.

Use an explicit absolute local date and IANA timezone:

```sh
telegram-archive-bot --config /private/config.toml daily-backfill \
  --from 2026-03-08 --through 2026-03-08
telegram-archive-bot --config /private/config.toml reader-build --limit 1
telegram-archive-bot --config /private/config.toml reader-status
telegram-archive-bot --config /private/config.toml reader-list \
  --date 2026-03-08 --timezone America/Los_Angeles --limit 100
telegram-archive-bot --config /private/config.toml reader-export MANIFEST_ID > day.json
telegram-archive-bot --config /private/config.toml reader-export MANIFEST_ID \
  --format markdown > day.md
```

`backfill` admits planner work; it does not itself prove provider coverage.
`reader-build` is local and returns admitted/processed/pending/error counts.
At most 64 historical windows and 64 activity observations are admitted per turn;
a durable keyset cursor and final bounded refresh prevent old partially indexed
windows from being certified complete. At most 16 affected scopes may be built
per explicit call (runtime uses one). Dirty work is FIFO. A build that exceeds
activity, record, node, byte or window limits retains an explicit generic error
and publishes no truncated head. Inspect `reader-status`; correcting reader
limits re-arms the affected work. Examples supporting 500 accounts can set
`daily.max_accounts = 500` and `reader.max_windows = 500`; initial admission and
publication still progress in bounded turns. `reader-list --after SCOPE` continues
its stable pagination.

## Manifest identity and enumeration

Every manifest pins the local date, timezone and UTC half-open interval, SOP,
window/account/config/provider identities, attempt state/reason and scan evidence.
DST days retain their actual 23 or 25 hours. A combined day points to account/day
manifests, then paginated indexes. Each reference has an immutable document ID,
content record hash, deterministic lookup title, and a page ID/URL only after
DA06 actual body readback. Index leaves have at most 16 activities; intermediate
pages have at most 32 child references. Follow `accounts`, `index` and `children`
recursively, reading each document's full managed captured-text JSON. Large
records/pages fail visibly at configured bounds; no tail is silently dropped.

Each activity occurs once in the portable exhaustive index. It retains every
associated window, the complete DA01 observation, separate activity/source times,
original URLs, immutable revision pins, root versus context-only role, capture
quality, missing reasons and context omissions. A repost can belong to today's
activity window while its original source is much older. Two days never adopt
one another's later activity observation. Within a window, observation time wins,
then the exported source-observation sequence resolves equal-second hydration/edits; an
identical retry does not advance that order. The index dispositions are `captured`,
`partial`, `unavailable`, `pending`, or `failed`. Duplicate references merge their
window IDs; immutable variants remain in the document closure, and exports retain
every revision body referenced by that selected closure before activity deduplication. Topic summaries
may merge repetition, but must preserve this exhaustive index.

Coverage fields have distinct meanings:

- `admission_complete` and `pending_activity_count`: historical local indexing.
- `collection_complete`: every included account window has explicit provider
  completion and local admission is settled. Failed/unknown windows stay incomplete.
- `body_complete`: all admitted activity bodies/context are captured with no
  recorded omissions or enrichment gaps.
- `published_activity_count`: activity/source pages with verified readback;
  `publication_verified` additionally requires all referenced index/account pages.
  This flag is the corpus closure; the selected root's own DA06 queue receipt is
  inspected separately. The root never depends on its own publication.
- `analysis_complete` in collector manifests stays false. `reader-checkpoints`
  separately reports external L1–L5 completion, reading counts and staleness.

An explicitly missing/context/link-only record can be fully published while its
body remains incomplete. PDF fallback, original/landing/direct URLs, upload and
attachment states, byte hash/remote references, gaps and deduplicated failure
report links remain visible. File/page presence alone proves neither bytes read
nor analysis completed.

The normal worker path supplies this evidence automatically. The reader joins
DA04 capture state, the selected observation's media limitations, DA07 PDF jobs
and verified readback, and DA08 reports. Root activity observations and DA04's
retained context observation IDs select the evidence; a later mutable source
revision cannot replace those pins. A missing body exposes capture state, reason,
cycle and attempt count in `SourceEnrichment.capture`. PDF verification requires
the retained original's exact digest. Later reconciliation can revoke that byte
proof, producing a new handoff while earlier handoffs remain readable.

Report references follow the selected private source/window streams, including
failed windows with no activities. A URL is included only after the reporter's
verified settlement; an unrelated source sharing the same defect fingerprint
does not acquire that link. Neither private reporter diagnostics nor watchlists
are copied from its event records. `reader_note_source` remains an explicit
supplement for other authorized producers, rather than a required deployment step.

Producer transactions enqueue a compact dirty input key atomically with their
state change, including publication/source observations, activity admissions and
explicit source enrichment. Each reader tick expands one key through the dependency
index or the indexed activity-to-window mapping (including windows without heads),
at most 64 scopes, using a durable cursor before building its bounded number of
heads. `reader-status.pending_inputs` reports remaining input keys; `pending`
includes those keys and dirty scopes. Restart resumes them, and recovery hold
pauses construction. Identical logical state remains a semantic no-op. Queries
use observation, stream and fingerprint indexes, never a scan of private event
JSON or an additional canonical body store.

Unchanged logical inputs do not create a new head/document. Late activities,
corrected pinned observations, verified child publication or typed enrichment
create immutable successors only for affected scopes and their combined day.
Earlier manifests/checkpoints remain readable. Downstream checkpoints for changed
heads become stale; unrelated days remain current. Observation insertion alone
cannot silently replace a pinned revision: the capture producer must append the
new activity/view association. Publication uses DA06 durable uncertainty handling;
this is not an exactly-once remote-create claim.

## Five layers and checkpoint contract

The authoritative baseline is **x-daily-briefing-v1 v1.0.0**. The deployment's
configured authoritative SOP page ID/URL and version travel in every handoff;
no chat history is required:

| Layer | Baseline output |
| --- | --- |
| L1 | Executive brief |
| L2 | Comprehensive topic map |
| L3 | Topic reports |
| L4 | Detailed evidence and per-item treatment |
| L5 | Full captured source archive, including explicit missing/link-only records |

External authorized readers may use `ntn`, connectors or other readers. Raw source
pages stay separate from their analysis outputs. A complete summary requires
all five layer checkpoints, exhaustive processed activity IDs and source reading
accounting; it does not turn incomplete collection into complete coverage.

`reader-checkpoint /private/checkpoint.json` imports one immutable external
assertion. `reader-checkpoints MANIFEST_ID` reports the current five layers.
Each `daily-reader-v1` checkpoint contains:

| Field | Required meaning |
| --- | --- |
| `session_id`, `layer`, `specification_version` | Stable reader session, L1–L5 number and layer specification |
| `manifest_id`, `input_manifest_sha256` | Exact manifest document key and captured root JSON SHA256 (`Record.id`) |
| `local_date`, `timezone`, `utc_start`, `utc_end`, `sop` | Exact resolved absolute window and SOP, copied unchanged |
| `state`, `outputs`, `cursor` | `in_progress` / `blocked` / `complete`; output page IDs with SHA256; resumable structured cursor |
| `processed_activity_ids`, `readings`, `unresolved_sources` | Exhaustive progress and explicit reading gaps |
| `provenance` | Worker, optional model, version and reading method |

Each reading pins `source_id` and optional `revision_id`; `state` is `fully_read`,
`partially_read`, `metadata_only`, `inaccessible`, or `out_of_scope`. Non-full
states require a reason. Partial/metadata/inaccessible source IDs must appear
in `unresolved_sources`; deliberate out-of-scope decisions remain explicit.
A full read requires reader/method and either `captured_text` SHA256 of the
pinned complete, nonempty body, or `verified_bytes` SHA256 matching the typed
verified attachment evidence. A title, preview, URL, block, filename or hash of
partial text does not satisfy it. These are external reader assertions checked
against retained evidence, not a bot audit of human cognition or remote outputs.
Alternative-reader details belong in provenance/method, and unresolved gaps stay
visible even if a layer finishes with explicit gap accounting.

A complete layer requires configured SOP, output references, exhausted cursor,
every admitted activity processed and every pinned source/revision in the selected
immutable documents assessed. An event admitted once across overlapping windows
can retain multiple selected revisions; each requires its own assessment.
`PortableExport::selected_sources()` exposes these references before activity
deduplication and never includes unrelated archive sources. Retries of
identical checkpoints are idempotent; every distinct version is retained. When
a head changes, default import refuses input drift. Resume the original absolute
snapshot deliberately with `--allow-stale`, or start against the new manifest;
never reinterpret a relative phrase such as "yesterday" in a later session.
`complete_summary` requires all five complete layers on the current input; read,
collection, body and publication coverage remain separately reported.

## Portable recovery and integration

JSON/Markdown exports are deterministic for the selected immutable corpus and
checkpoint history. They include full captured source text and metadata, original
links, managed document closure and gap accounting. They exclude config snapshots,
runtime tokens, unrelated private records, jobs, chat state and publication
attempts. Binary attachments are not embedded; URLs and byte/read availability
remain explicit. Export may fail visibly at `export_bytes`/`semantic_files` bounds.
The actual pretty JSON (including its terminating newline) or Markdown (including
repeated source-body sections) must fit `export_bytes`. Rendering uses a bounded
buffer and finishes before any stdout is published; compact JSON size alone is
not an output bound. Library callers use `render_json(max_bytes)` or
`markdown(max_bytes)` with an explicit rendered-byte cap.

Full backup preserves operational head/dependency/cursor/enrichment/checkpoint
state. Semantic Git export preserves existing `notion_documents` (including
manifests and checkpoints), canonical sources/activities and their immutable
closure, without operational publication receipts. A known manifest ID remains
exportable after semantic restore. Runtime heads/dependencies are re-derived
after explicit recovery release. Both restore paths retain recovery hold;
`reader_tick` pauses and `run` refuses activation under hold. Follow
[recovery](recovery.md) before releasing external work.

Producer integration uses `Store::reader_note_source(&SourceEnrichment)`;
`source_id` and optional pinned `revision_id`, attachment states/links/proofs,
gaps and failure-report URLs are canonicalized before targeted invalidation.
It never changes the original revision. DA04 must append the hydrated activity
view/observation. DA07 and DA08 provide attachment/report evidence through this
seam. This isolated DA09 leaf reserves migration019 after015; parent serial
integration must execute actual016,017,018 before019 and validate fresh14 upgrade.
No placeholder schemas or competing capture/PDF/report workers are installed here.

Offline acceptance uses actual SQLite, CLI commands, ConfigManager signals,
committed Git/backup roundtrips and a fake `ntn` child with body readback. It covers
DST, duplicates, old reposts, failed accounts, Article text, PDF link fallback,
429 recovery, failure-link deduplication, hierarchical pagination and read-state
refusal. These fixtures are synthetic. The bounded actual free-provider and
Notion workspace traversal remain separate live gates owned by deployment;
missing authorization is pending live acceptance, never a fake remote success.
