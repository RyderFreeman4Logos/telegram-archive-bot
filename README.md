# Telegram Archive Bot

Project-Spec: rust-telegram-archive-bot-v1

A new Rust service for durable Telegram source archives and validated audio.
SQLite and immutable content-addressed files are authoritative; a separately
configured Git repository receives a deterministic semantic projection.

**Status: 0.1.0-rc.3 candidate; client playback validation is pending.**
Core requirements are [F01–F20](docs/implementation/issue-map.json). Search (F21)
and legacy import (F22) are optional. This repository has fresh history and
contains no prior bot implementation or archive data.

Read the [release notes](docs/release-notes-0.1.0-rc.3.md) and
[RC1 provider/client evidence](docs/verification/f20.md) before activation. RC3
publication requires source-bound verification assets; their final preparation
is pending at this source snapshot.
The RC1 real model/translation/TTS, download and three Telegram audio checks passed;
Desktop seeking and speed controls remain blocked by client access; Android
playback is pending confirmation of the phone's test recipient.

## Build and offline checks

Install Rust through rustup, then clone this repository and run:

```sh
cargo build --locked
cargo build --locked --release --manifest-path integrations/hymt-bridge/Cargo.toml
cargo run --locked -- --help
cargo test --locked
cargo fmt --all --check
cargo fmt --manifest-path integrations/hymt-bridge/Cargo.toml --check
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --manifest-path integrations/hymt-bridge/Cargo.toml --all-targets -- -D warnings
```

The pinned toolchain is 1.99.1; the minimum supported compiler policy starts at
1.99 and is raised explicitly with dependency changes. CI uses the pinned version.
Offline tests require no provider credentials. Runtime adapters require FFmpeg,
ffprobe, yt-dlp, md2audio and Bubblewrap (Linux namespaces); PDF extraction uses Poppler. See the
[specification](docs/spec/root.md), [architecture](docs/spec/02.md), and
[dependency inventory](docs/dependencies.md).

Translation uses the separately built `archive-hymt-bridge` Rust binary and the
[pinned hymt libraries](docs/translation.md). Build it before the integrated tests;
they exercise the actual binary against a local HTTP fixture without credentials.

Never put bot tokens, cookies, personal archives or live configuration in this
repository. The new service has an authorized trial deployment. Real client playback
validation remains separate from compilation and deterministic testing.

Producer and delivery contracts are documented in [speech](docs/speech.md),
[media download](docs/media-download.md), [audio finalization](docs/audio-finalization.md)
and [durable delivery](docs/delivery.md). The durable service dispatcher integrates
these adapters. Follow [installation and operation](docs/operations.md) for an
isolated offline replay, doctor/status, backup/restore, reload and Linux user
systemd deployment. `run` explicitly activates a live Telegram poller; build,
installation and replay do not activate it. Required real-provider and client
acceptance remain separately recorded in F20.

New application code is licensed under MIT OR Apache-2.0.

See [semantic Git projection](docs/git-projection.md) for the independent archive destination, event recovery and separate push state.

Use [verified backup and recovery](docs/recovery.md) before moving canonical state; Git-only recovery has explicitly narrower scope.
