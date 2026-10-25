# Notion archive format v1

**Spec-ID: DA05** — [#72](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/72),
depending on [DA01 #68](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/68).
Local SQLite/CAS remains intake/recovery authority. Notion availability is
reported only after the returned remote body has been verified. This format
requires no access to the originating workstation for ordinary text reading.

`notion::Record` contains a schema version, immutable record ID, title, capture
time, every original URL, retrieval/extraction provenance, completeness and full
captured UTF-8 text. Original byte SHA256 is separate from captured-text SHA256
and the canonical Notion projection digest. A missing raw digest stays absent.

| Record kind | Versioned metadata |
| --- | --- |
| Account | Complete DA01 account observation, including ProviderId or Unresolved identity, optional handle/display name and evidence |
| Source | Provider and external identity, with original URLs in the common envelope |
| Revision | Source ID, revision ID and optional authored time; Record ID equals revision ID |
| Activity | Complete DA01 activity observation: event/actor/source identities, activity kind, revisions or missing reason, discovery, separate activity/source times and evidence |
| Relation | Source ID, target source ID and the existing typed source-relation kind |
| Daily manifest | Absolute day bounds/timezone, accounts, exhaustive record references and explicit coverage/gaps |
| Agent output | Manifest ID, SOP version, L1–L5 number and source references |
| Checkpoint | Manifest ID, SOP version, completed levels and remaining record IDs |

Account/activity observations use a `Da01Observation { schema_version: 1, value }`
wire envelope. `value` is the entire serialized DA01 observation, not a reduced
mapping: Unknown/Instant/Interval times, precision, nanoseconds, unresolved IDs
and provenance survive unchanged. `Da01Observation::from_account` and
`from_activity` accept the actual DA01 `AccountObservation` and
`ActivityObservation` types. They serialize the complete observation and retain
its version; the publication planner rejects unsupported envelope versions.
They do not duplicate DA01 enums or guess missing timestamps. Offline tests use
real `Store::record_activity`, then edit/hydrate the source and reopen storage
before converting the older observation and reading its planned Notion metadata.
Known and unresolved actor/event identities, unknown/instant/interval time
evidence, nanosecond precision, provenance and immutable revision/view pins must
survive exactly. This establishes typed local conversion, not live publication.

Stable publication keys hash `[schema_version, record_kind, immutable_record_id]`.
The page title contains this key for later reconciliation. Verifying an existing
page against a changed body yields a remote-content conflict; verification never
overwrites that page. New revisions/observations require their own immutable IDs.
`create` is a single create attempt, not a lookup/upsert: its caller must own the
durable intent and uniqueness decision before calling it.
There is no inferred Notion idempotency-key parameter and no exactly-once create
claim. DA06 persists intent before dispatch and resolves uncertain creates.

The ordinary block sequence is:

1. Managed start marker and readable JSON metadata, including original URLs.
2. A `Captured text (managed)` heading and every captured character in order.
3. Managed end marker and an explicit unmanaged annotations/analysis heading.

Paragraph chunks contain at most 2000 UTF-16 units, conservatively satisfying
Notion's 2000-character limit even for astral characters. Newlines, tabs, combining
marks and control characters are preserved in JSON; no Markdown interpretation,
Unicode normalization, inserted separators or text truncation occurs. Server
rejection remains a visible failure. Requests contain at most 100 children and
never exceed the configured payload cap (maximum 500000 bytes). Large records
use sequential bounded append requests; a partially written page is not complete.

Canonical version `notion-ordered-text-v1` is the UTF-8 JSON serialization of
ordered managed `{kind,text}` pairs, through the unmanaged-section heading.
Readback fetches the actual page and paginated block children, checks page and
destination identity/title, reconstructs all managed rich-text content and
compares every ordered pair, then recomputes the digest. Missing/truncated,
reordered, unexpected, child-containing or altered managed blocks are conflicts
even if all stored digest markers remain unchanged. Notion rich-text styling and
link attributes do not change the text projection. Refreshed signed attachment
URLs are not part of this text schema; binary/PDF publication is DA07.

Anything after the unmanaged heading belongs to humans/downstream agents. The
adapter never patches existing content or deletes annotations: `verify` is
read-only. An edited managed section returns conflict with its page ID. Complete
body verification is required; an echoed digest or local path is never proof.

`Adapter::create` uses the supervised `Tool` with structured argv and a mode-0600
private request file, followed by GET verification. All temporary state is
disposed on return. The selected credential is checked against original decoded
record strings before chunking, including JSON escaping and chunk-boundary cases.
Unknown/malformed responses and lost write responses do not produce availability
receipts. Explicit structured 401/403, 429/529 and conflict responses have distinct
states; unknown errors remain conservative. Numeric exposed retry delay is
retained; no Retry-After value is fabricated from human-readable error text.

Offline fixtures exercise the actual child process and JSON/file boundary only.
They do not contact Notion, prove live workspace authorization or establish
autonomous publication. Live target/auth and DA06 worker integration remain
separate acceptance receipts. See [configuration.md](configuration.md) for setup.

## Automatic publication and recovery

**Spec-ID: DA06** — [#73](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/73).
`run` starts a separate Notion worker even when Telegram, models, translation and
TTS are disabled. It also maintains the local outbox while Notion is disabled or
unconfigured. Source observations, immutable revisions, accounts, activities and
explicitly saved derived documents enqueue in their canonical transaction.
Network and child processes run only after the publication attempt commits.
A per-store OS lock serializes workers and operator controls; it is released on
process death. No lease expiry is treated as permission to create another page.

Existing canonical records are discovered through durable keyset cursors, at
most 64 records per worker turn (the public bootstrap API permits 1–256). Empty
namespaces are marked done; insert triggers cover later intake. Missing tool,
token or destination never prevents local capture. The first explicit destination
binds up to 64 previously unbound intents per turn. Once bound, an intent never
moves when configuration changes. Returning to its original destination resumes
that destination's backlog. `SIGHUP` cancels and settles an in-flight operation
before using the new Notion configuration. An operation that may already have
written remains uncertain; disabling does not erase that evidence.

The outbox pins the full versioned record, deterministic publication key,
canonical expected body digest and destination before dispatch. Attempts,
known page ID, append/readback progress and verified block IDs are durable.
Source observation documents retain metadata, evidence and typed relations;
revision documents contain the exact source text. Source capture times use
explicit Unix-second decimal strings from SQLite, while DA01 event/source time
evidence remains in its original typed envelope. Unknown times are not guessed.
`Store::record_notion_document` persists DailyManifest, AgentOutput and Checkpoint
records separately; identical records are idempotent, while changing the body
under the same identity is an error. Its canonical format bound is the default
DA05 record limit. Producers requiring new content must supply a new immutable ID.

An interrupted create becomes `reconcile`. A known page is fetched directly;
an unknown page is searched by deterministic title, with bounded pagination,
then its destination, key and actual ordered body are verified. An empty search
keeps the intent uncertain and schedules another read-only attempt. Duplicate
markers, partial bodies or human edits become visible conflicts. The worker never
recreates an uncertain page, resumes appending to a partial page, deletes remote
content or overwrites a human edit. This is not an exactly-once delivery claim.
Known no-effect failures use bounded create attempts and retry delay. Unsupported
or missing prerequisites do not exhaust the create budget. Auth/rate errors and
provider retry hints remain separate from ambiguous writes.

`status` exposes `notion_publication` counts, oldest pending age, unbound count and
remaining bootstrap namespaces. `doctor.notion` reports local prerequisites
separately from local archive readiness; neither command contacts Notion.

- `notion-list --limit 20` lists safe identities, state and errors, without bodies
  or credentials. The maximum is 100 rows.
- `notion-reconcile ID [--page-id UUID]` schedules read-only verification. A
  supplied page is explicitly untrusted and cannot become the known page until
  destination, deterministic key and complete body verification succeed.
- `notion-retry ID` re-arms only a known no-effect failure. It cannot turn an
  uncertain or previously completed write into permission to create again.

Operator changes are audited locally. Global recovery hold takes precedence.
Full backup retains queue, attempts, progress and verified receipts, but every
unfinished intent becomes uncertain on restore: an older snapshot can predate a
successful remote write. Git exports include semantic derived documents, not
Notion operational receipts or credentials. Git-only restoration starts under
hold and treats imported publication work as uncertain. After explicit release,
only remote reconciliation can establish availability; regenerating metadata
that differs from an older publication yields a conflict, never a blind recreate.
Raw source evidence remains in canonical SQLite/CAS; Notion is a separate sink.

Offline acceptance uses real SQLite transactions, public CLI `run`/`replay`, real
signals, fake ntn child processes, backup and committed Git roundtrips. It covers
missing prerequisites followed by automatic drain, disabled zero calls, target
pins, accepted-response loss, empty search, cancellation/restart, partial writes,
auth/rate failures, remote edits and operator-supplied wrong pages. These fixtures
do not prove production Notion worker publication or deployed-runtime behavior;
those require their own candidate-bound live receipt after dependency integration.
