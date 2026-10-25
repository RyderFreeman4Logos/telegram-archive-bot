# Daily original-content capture

Spec-ID: DA04. Issue: [#71](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/71).

This opt-in worker hydrates the X source IDs already recorded by account/day
collection. It uses the existing canonical sources, immutable revisions and CAS;
there is no separate daily body store. A repost activity points to the original
post, while each account/day/activity keeps its own immutable observation and
selected context view. Timeline observations remain unchanged after hydration.

## Capture without a user account or derivatives

The known-ID FxEmbed fallback needs no user account or paid API. Configure the
existing source adapter and capture worker explicitly:

```toml
[telegram]
enabled = false
[model]
enabled = false
[translation]
enabled = false
[synthesis]
enabled = false
[sources]
x_fallback_enabled = true
[daily.capture]
enabled = true
max_admissions = 100
max_links = 8
refresh_seconds = 86400
retry_seconds = 300
```

`run` accepts capture-only operation with `daily.enabled = false` and Telegram
disabled. The daily planner/timeline adapter separately discovers activities;
turning capture on does not enumerate accounts itself. This worker creates no
model, translation, speech or Telegram jobs/outbox entries. Optional local Git
projection and later Notion consumers can read its normal activity/view records.
Archive push remains separately configured; recovery hold blocks this worker.

Where enabled, FxEmbed is tried first. Existing official/anonymous Article
adapters remain available under their existing configuration and selective
Article rules. The provider payload is retained in CAS. Ordinary post text,
full available Article text and embedded quote previews remain distinguishable.
An Article preview is Partial, and absent/forbidden/deleted/error responses stay
explicit. Media attachments are retained as references with a visible
`media_references_not_transcribed` gap; capture does not claim their transcript.

Expanded URL entities (including FxEmbed v2 `raw_text.facets` replacements and
Article LINK entities) supply external source URLs. The
[FxEmbed API introduction](https://docs.fxembed.com/api/introduction/) and
[upstream OpenAPI schema](https://github.com/FxEmbed/FxEmbed/blob/main/docs/specs/fxtwitter-openapi.json)
describe that provider surface. URLs are normalized, fragments and common
tracking parameters removed, and canonical duplicates coalesce. The existing
Documents pipeline retains raw HTML/PDF and extracts normalized text. Optional
browser rendering follows its existing sandbox configuration. Extraction failure
is a visible source gap and never deletes already retained originals.

## Explicit context and limits

A view follows typed quote/reply/repost references and bounded reply ancestors.
It also includes already discovered replies by the same verified provider account
as a non-repost parent. An unresolved handle is insufficient proof of common
authorship. This is known context, not enumeration of all user replies. No extra
search/timeline crawl is implied. Later discovered known continuations can
refresh a completed view while reusing the cached root.

Each view records the selected UTC bounds and marks context as `inside_window`,
`outside_window` or `unknown` using source creation time. Old source text reposted
on a new day remains associated with that day's activity without rewriting its
source date. A missing context target retains its edge and omission.

`limits.graph_nodes`, `graph_depth`, `reply_ancestors`, `input_bytes` and
`download_seconds` bound each traversal. `daily.capture.max_links` separately
bounds eligible external documents (0 disables external extraction). Every
node/depth/reply/link/byte omission is retained in the view. Raw provider evidence
can remain in CAS when normalized text exceeds the remaining graph budget; that
text is omitted and never labelled Complete. `retrieval.bytes` counts
the content/raw admitted to the view, with raw-only gaps recorded separately.

## Durable admission, reuse and operation

A committed DA01 activity observation is the durable enqueue boundary. An indexed
rowid cursor admits at most `max_admissions` observations per tick (1–1000),
including every origin for a shared source. Generated hydration observations
advance the cursor but do not recursively enqueue themselves. Source work is
coalesced globally; finished bodies and context nodes are reused across origins
and retries through canonical observation pointers.

One durable global capture lease serializes collection. Attempts pin a complete
configuration snapshot; a reload affects new source cycles. A restart resumes
checkpoints, retains finished nodes and records the interrupted attempt. A stale
lease cannot publish. Attempt exhaustion includes interrupted attempts and does
not silently fetch again. Safe aggregate status and a scoped retry are available:

```sh
telegram-archive-bot --config /private/daily.toml daily-capture-status
telegram-archive-bot --config /private/daily.toml daily-capture-retry SOURCE_SHA256
```

Retry accepts a failed/blocked source digest, starts a new cycle under current
configuration and preserves prior attempts, revisions, observations and the source cache.
An unchanged configuration resumes the checkpoint. A changed configuration rebuilds
the traversal from retained cached captures so the new node, byte and context
limits apply; it does not carry over the old graph budget.
Transient failures back off by `retry_seconds` (1–86400), with
`limits.max_attempts` attempts per cycle. Completed sources become eligible for a
new snapshot after `refresh_seconds` (1–31536000, default one day). This permits
edits/new revisions; capture is not permanently once per ID. Earlier activity
revision pins stay immutable. Public logs/status contain counts/digests and
static reason categories, never raw source URLs or provider error strings.

Schema 016 adds scheduling/attempt/origin/cache pointers and an indexed reverse
reply lookup. Full backups retain these tables. Restore expires collection
leases, records interruption and retains recovery hold until explicit release.
Semantic Git export retains canonical source/raw records, activity observations,
revision-pinned views, omissions and bounded attempt provenance; operational
capture leases/cursors are recovered by full backup, not semantic export.
Integration must apply the separate real migration 015 before 016; this isolated
leaf's schema14-to16 fixture does not attest the combined 14-to15-to16 upgrade.

Synthetic fixtures exercise the actual autonomous worker, shared repost sources,
known self-thread context, unavailable quotes, full/partial Articles, expanded
links, real HTML/PDF extraction, limits, refresh, cancellation, contention,
backup/restore hold, public status/retry and local Git export. They make no live
provider, Telegram or Notion acceptance claim.
