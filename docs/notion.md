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
