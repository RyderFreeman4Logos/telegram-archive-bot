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
Notion's 2000-character limit even for astral characters. Ordinary newlines, tabs
and combining marks retain their exact text. No Markdown interpretation,
Unicode normalization, inserted separators or text truncation occurs. Requests contain at most 100 children and
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

**S87** — [#87](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/87):
real Notion readback accepted PDF text but omitted its final U+000C (form feed).
The local canonical text and SHA256 remain unchanged. Text containing C0 controls
other than LF/tab, DEL or C1 controls uses canonical version
`notion-ordered-text-json-v2`. This also applies when serialized metadata contains
literal DEL/C1 characters. Ordinary v1 plans remain byte-identical.

The explicit metadata field `content_encoding: "json-string-v1"` and heading
`Captured text (managed; JSON string encoding)` identify this representation.
Its content paragraphs contain one JSON string, split only to satisfy the same
2000-unit bound. All controls in that string are escaped; remaining literal
DEL/C1 in metadata JSON are escaped too. A reader concatenates the content
paragraphs **without separators**, JSON-decodes the string exactly once, then
checks the recovered UTF-8 against `content_utf8_sha256`. Literal backslashes,
quotes, astral characters, combining characters and every original control
survive this decode. The actual readback verifier performs that decode and hash
check after validating every returned block; an echoed metadata hash is not proof.
Both the original serialized record and the expanded metadata plus encoded text
must fit `notion.record_bytes`; block/request limits also apply before dispatch.
Oversize representation is a visible failure, never silent truncation.

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
If a planner upgrade changes a pinned representation, the worker records
`publication_plan_changed` as a conflict before invoking ntn. It preserves the
old record, key, expected digest, page and attempt history, allowing later queue
items to proceed. It cannot silently adopt v2 for a v1 intent, recreate an
uncertain page, or overwrite human edits. Existing S87 conflict pages remain
conflicts; a new live acceptance run requires its own explicit recovery decision.
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

## PDF originals and reader access

**Spec-ID: DA07** — [#74](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/74).
A retained PDF observation publishes its original observation/evidence, portable
`da07-pdf-v1` metadata and available extracted text as ordinary readable blocks.
This publication finishes before binary work is admitted. Metadata distinguishes
source/canonical URLs, known originating post/article landing URLs and the direct
download URL. It records source/revision/observation identity, original byte MIME,
length, SHA256, filename/title, retrieval time, validation and extraction status.
No extractable text remains `no_extractable_text`; it is never inferred from a
successful upload. Direct URLs with signed/expiry parameters are temporary;
other download URLs have unknown stability. External access is not guaranteed.
Landing references are bounded to 128 distinct sources with an exact omitted
count; original canonical records retain all relations.

Each immutable observation page has its own deterministic attachment intent,
pinned to that page, destination and original digest. Repeated worker turns and
restarts reuse that intent. A later observation/revision has its own page and
intent; it does not overwrite earlier metadata or binary proof. Existing verified
observation pages are admitted through a durable keyset pass of at most 64 rows
per turn. The normal DA06 completion transaction admits new PDF pages atomically.
All PDF work shares the DA06 OS lock, durable queue and attempt history. It pauses
under recovery hold, cancellation, disabled configuration or missing prerequisites.

The pinned ntn 0.22.10 generic API supports single-part `file_uploads` creation,
`POST file_uploads/ID/send --file PRIVATE_COPY` and a PDF child block containing
the upload ID. The private copy comes from verified retained CAS bytes, not a new
origin download or PDF extraction. The PDF-only subprocess directory has a
bounded 24 MiB allowance; ordinary text operations retain their 16 MB allowance.
Only validated originals of at most 20 MiB are attempted. Oversize, unavailable or
rejected/unsupported uploads leave readable text and an explicit binary gap;
no multipart capability is claimed. Auth and transient failures remain retryable.

Upload and attachment are separate states. Upload ID/expiry and each write's
intent are committed before dispatch. An uploaded file is attached before expiry.
Readback checks the stable page's destination, locates the deterministic PDF
caption through bounded pagination, reads the stable block and retrieves the
fresh file URL with the public-only HTTP client, without the Notion credential.
Availability requires actual bytes, byte length and SHA256 to match retained CAS.
An echoed upload ID, caption, append result or digest is insufficient. Failed or
missing file readback revokes a previous local availability proof. `verified_at`
is the time of that observation, not a promise of permanent reader access.

The page's separate `DA07 availability` paragraph is owned by this worker; other
unmanaged annotations remain untouched. It exposes upload/attachment states,
stable page/block/upload IDs, verification time/digest and truthful gaps. This
paragraph is read back after a write. Text/page-only readers can use the original
record and URL fallback even when they cannot read files. A separately configured
agent needs no local CAS path, private bot database or bot credential to interpret
this versioned contract; its own Notion/origin permissions still govern access.
Signed Notion file URLs are refreshed by stable block ID and are never the only
durable citation or stored as permanent download pointers.

An unknown PDF/status append is reconciled by its marker, never blindly appended
again when absent. An unknown upload creation without a returned ID likewise
remains a visible gap rather than creating another upload. A known expired,
unattached upload can be replaced from the same retained bytes. Safe automatic
write attempts use the configured budget and backoff; exhausting it requires an
operator decision. `notion-reconcile ID` is strictly read-only for PDFs, including
status blocks. `notion-retry ID` can re-arm PDF continuation only when no attachment
append, status append or upload creation has an unknown identity. It cannot turn
an uncertain attachment into a new write. Local original bytes remain retained.

`notion-pdf-status --limit 20 [--after LAST_QUEUE_ID]` reads portable metadata and
availability without network calls. Unlike ordinary aggregate `status`, this
explicit content command includes original URLs. Limits are 1–1000; results are
ordered by queue ID and the last returned ID is the continuation. The public
`Store::notion_pdf_availability_after` API provides the same bounded contract.
Manifest producers include its binary gap separately from text coverage using
DA05 `Coverage.gaps` and stable source/revision/page references. A failed PDF job
backs off independently and does not block other records or manifest publication;
DA09 owns automatic day-manifest construction.

Migration 017 extends the DA06 outbox. This isolated DA07 leaf is based on schema
015 and deliberately contains no placeholder 016. Integration must include the
actual DA04 migration 016 before 017 and verify a fresh schema-014 upgrade before
deployment. Full backup retains PDF progress/attempts and original CAS under the
existing recovery hold. Restoring an older backup reconciles an already attached
remote file before writing. Semantic restoration rebuilds operational work and
scans deterministic markers before first attachment; it does not infer remote
availability from local metadata.

Offline acceptance uses synthetic ntn subprocesses, real SQLite/CAS, a local
HTTP byte server and the actual durable worker. It covers success, upload/append
response loss, absent markers, unsupported/oversize/auth/transient failures,
missing/corrupt attachments, no text, upload expiry, signed URL refresh, old backup
restore/hold, bounded old-page admission, read-only controls and manifest gaps.
It does not establish live Notion authorization, a real workspace attachment,
reader provider compatibility or deployed-runtime behavior.

The S87 offline regressions additionally model control removal at the real child
process readback boundary, independently decode all C0/DEL/C1 text plus Unicode,
enforce expanded byte limits before any child invocation, and reopen an old v1
intent to prove its pins survive without blocking later work. The retained first
live PDF run failed text verification and performed no PDF upload. These offline
results do not turn that run into a success or establish corrected live access.
