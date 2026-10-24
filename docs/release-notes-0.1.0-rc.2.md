# 0.1.0-rc.2

This candidate adds the requested legacy-bot parity and historical reply
operations. Reply to an existing Telegram message or supported file with
`/translate`, `/tts` or `/summarize`. The selected operation is persisted, outputs
reply to the initiating message, and originals remain immutable. `/get JOB`
resends retained results without repeating provider work. `/start` and `/help`
describe the supported commands.

The Linux service uses Rust, ADK-Rust 2.3.0, the pinned hymt Rust bridge, yt-dlp
and md2audio. [Legacy parity](legacy-parity.md) maps the original handlers to
the implemented paths. [Operator commands](operations.md#operator-url-and-local-files)
support a single URL and read-only local-file summary batches. Optional Chromium
rendering uses a private profile and public-only network policy. Optional X
guest article enrichment retains its provenance and explicit partial results.
Default narration and ordinary tracks still require full decode and measured
audio duration before delivery.

## Storage and replay evidence

SQLite plus immutable content-addressed files remain canonical. Connection reuse
reduces repeated WAL checkpoint writes while keeping FULL synchronization.
Schema 12 indexes source observation lookups. Semantic Git projection remains
an independent, recoverable event stream with explicit remote-push state.
Large Git-only restores can set a measured positive `limits.semantic_files`;
the 100,000 default and independent byte, path and committed-object checks remain.
See [storage measurements](verification/storage-performance.md),
[Git projection](git-projection.md) and [recovery guarantees](recovery.md).

The read-only private inventory contains 6,761 files. Reconciliation accounts for
22,482 dispatched raw HTTP candidates and 23 invalid-shape probes, with five
overlaps: 22,500 distinct candidates. Capture-only bulk replay disables Telegram
sends, model generation, translation, narration and remote pushes. Corrective
browser/error runs preserve the original attempts. Repeated input reuse and
integrity are checked independently. A completed job, HTTP 200 or retained body
is not proof of human-reviewed semantic completeness.

At this pre-publication source snapshot, final RC2 installation/runtime
attestation, the full main-corpus Git export/restore, asset publication and
download verification remain pending. Publication requires a
`release-verification.json` and verification archive with the final source-bound
gate, review, installed/runtime identity and sanitized corpus accounting. A
post-publication receipt must separately verify the downloaded assets. Each
historical run keeps its actual executable digest; results must not be reassigned
to a newer binary. Public assets must exclude raw URLs, private bodies, filenames,
credentials and recipient identifiers. Prior failed receipts remain in the
private evidence store.

## Installation and rollback

Follow [installation and operation](operations.md). The application package uses
version 0.1.0-rc.2; the unchanged pinned hymt bridge retains version 0.1.0-rc.1
and its separately attested executable. The planned Linux x86_64 pair inherits the build host's
`target-cpu=native` for Intel Alder Lake; use Cargo source installation on other
CPUs. External tools are installed separately. Publication must include notices,
dependency inventories and the applicable MPL dependency source archive.

Take and verify a full backup before opening existing state. Migrations are
forward-only: a schema-11 executable cannot reopen schema 12. Rollback restores
the verified pre-upgrade backup into a new directory with the old binary.
Restored state begins under a recovery hold; reconcile pending or uncertain
effects before activation. Local Git export is not a backup of raw/binary bytes
or operational queues. Defaults do not automatically evict retained history.

The new service has an authorized trial deployment. The legacy Python service,
draft files and their Git history remain unchanged.

## Explicit limits

Desktop 4.6.5 and Android Telegram 10.14.3 seek/speed controls remain unverified
under [issue #41](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/41).
Desktop lacks an authorized graphical session; the unlocked Android account
differs from the allowed recipient and adding it awaits confirmation. Historical
reply handling has controlled Bot API coverage; an actual user-client roundtrip
remains unobserved and requires a separate receipt.

Official X API acceptance lacks its bearer credential. Public fallback success
does not supply that credential or guarantee all posts/articles. One selected
valid YouTube video was reported private by yt-dlp and requires owner-authorized
account access; this is not a diagnosis of every media failure. Provider denials,
deleted/missing sources, malformed output and unsupported inputs remain explicit.

RC1 real model/hymt/TTS and three Telegram audio receipts retain their original
source identities in [F20 evidence](verification/f20.md). They do not establish
client controls, remote model-weight identity or human semantic/audible quality.
Live local/multi-speaker TTS and real remote Git push remain unperformed; their
controlled fixture coverage is separate. F21 search and F22 legacy import remain
optional backlog. This candidate is not described as fully production-validated.
