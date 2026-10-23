# 0.1.0-rc.1

This is a public release candidate, **not production-validated**. Core F01–F20
implementation is present. Desktop/Android seeking and playback-speed acceptance
is pending client access; official X live acceptance lacks a bearer credential.
See the [performed/blocked evidence](verification/f20.md),
[feature audit](verification/feature-acceptance.md),
[checklist](verification/release-checklist.md) and
[machine-readable versions](verification/capabilities.json).

## Supported behavior

Linux service with authorized durable Telegram ingress, immutable source
revisions/pinned graph views, X fallback, web/text/PDF originals, ADK-Rust
metadata and recoverable workflows, direct pinned hymt Rust translation,
supervised yt-dlp downloads and md2audio narration. Completed outputs survive
worker restarts. Canonical state is SQLite plus immutable files; a separate Git
archive is a deterministic projection. Full backup and narrower Git-only rebuild
are supported. Unknown sends require explicit acknowledged resend.

The default narration is validated Ogg/Opus `voice_note`. Ordinary recordings use
validated MP3/M4A `audio_track`. Full decode and sample-measured duration are
required before delivery. Actual test-chat receipts confirmed a 104-second voice,
1182-second track and 1474-second track. These do not establish client controls
or human-audited semantic/audible quality.

## Installation, requirements and limits

Follow [installation and operation](operations.md). Source builds use Rust 1.99.1,
ADK-Rust 2.3.0 and hymt `de670b21b26b3a88a8d711845c9e3d3b9af4c0ba`.
The Linux x86_64 binary pair is built/tested on Debian 12; other host architectures
are not release-tested. External yt-dlp, md2audio, FFmpeg/ffprobe, Poppler, Git and
Bubblewrap are installed separately. Their exact tested versions and executable
hashes are recorded in the capability manifest. The archive contains both Rust
binaries, application licenses, third-party notices, dependency inventory and
this validation documentation; MPL source packages are a separate attachment.

Keep state on a local filesystem and use separate private state/archive roots.
Default delivery limit is 49 MB and media duration limit is two hours; oversized
media fails clearly. Work, output, phase, request and whole-job budgets are finite
and configurable. No automatic history eviction exists: monitor retained bytes.
The production host must permit the configured unprivileged namespace boundary;
the application never changes system policy to enable it.

Take and verify a full backup before upgrades. Migrations are forward-only.
Rollback selects the old executable and restores its pre-upgrade backup into a
new directory; do not run an old executable against a newer live database.
Restored state starts held, with old effects paused and ambiguous sends unknown.
Inspect/reconcile before explicitly starting the service. Installation, replay
and doctor do not start a Telegram poller or replace the existing bot.

## Known gaps

Desktop 4.6.5 and Android 10.14.3 playback controls remain untested because the
graphical session is unauthorized and the connected phone is locked. The exact
matrix remains tracked in issue #41. FxEmbed returned partial data for the live X
samples; provider availability varies. Remote model weights/defaults were not
attested and translation semantic fidelity is not guaranteed. Local/multi-speaker
TTS has fixture coverage only. Live remote Git push and a production poller
switch were not performed. Search (F21) and legacy import (F22) are optional
backlog; no legacy history or private drafts were imported.
