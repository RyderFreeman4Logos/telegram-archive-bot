# Installation and operation

Linux is the supported service host. Use a local filesystem, Rust 1.99.1 and
FFmpeg/ffprobe, Git, Poppler and Bubblewrap versions listed in the release
capability manifest. Install tools deliberately and pin their versions; never
update them during a job. See the [license inventory](dependencies.md). Containers
and a queue broker are not required.

## Install from the public checkout

```sh
git clone https://github.com/RyderFreeman4Logos/telegram-archive-bot.git
cd telegram-archive-bot
cargo install --locked --root "$HOME/.local" --path .
cargo install --locked --root "$HOME/.local" --path integrations/hymt-bridge
telegram-archive-bot --version
archive-hymt-bridge --version
```

On Debian/Ubuntu the system packages are `ffmpeg poppler-utils git bubblewrap`.
Install yt-dlp at the audited release and md2audio at the audited commit into
isolated Python environments through your tool manager. Configure their **actual
absolute entry points**, including an interpreter environment with dependencies;
a shell-manager shim requiring interactive startup is not a service executable.
Doctor records resolved executable hashes and version/help output. The bridge
links pinned hymt Rust libraries directly; no hymt CLI is used. Optional missing
executables disable their requested synthesis, translation, browser or media feature at
service startup with a `capability_disabled` event. Invalid required configuration,
unreadable configured secrets or a configured unavailable Git destination still
fail early. PDF extraction can fail partially while retaining a usable PDF.

For optional JavaScript rendering, deliberately install and pin Chromium or
Chrome, configure its native executable and enable `sources.browser_enabled`.
Only `/usr` and `/opt/google/chrome` browser installations fit the supported
runtime mounts. No user profile is exposed. See [document extraction](documents.md)
for the network, profile and provenance limits.

## Reproducible offline workflow

Only the application, Git and the checked-in synthetic updates are needed for
this text-only demonstration. Run from the checkout after installation:

```sh
demo_root=$(mktemp -d /var/tmp/archive-demo.XXXXXX)
cat > "$demo_root/config.toml" <<EOF_CONFIG
state_dir = "$demo_root/state"
[telegram]
bot_id = 1
allowed_chats = [2]
allowed_users = [3]
[archive]
directory = "$demo_root/archive"
push = false
EOF_CONFIG
telegram-archive-bot --config "$demo_root/config.toml" check-config
telegram-archive-bot --config "$demo_root/config.toml" replay examples/offline-updates.json
telegram-archive-bot --config "$demo_root/config.toml" verify
telegram-archive-bot --config "$demo_root/config.toml" doctor
telegram-archive-bot --config "$demo_root/config.toml" status
telegram-archive-bot --config "$demo_root/config.toml" backup "$demo_root/backup"
telegram-archive-bot verify-backup "$demo_root/backup"
telegram-archive-bot restore-backup "$demo_root/backup" "$demo_root/restored"
```

Expect two complete processing jobs, one unchanged source revision and two
pending independent Markdown deliveries. Replay never polls or sends Telegram
messages and refuses Telegram-enabled/push-enabled configuration. Other explicitly
enabled source/model/TTS providers **can** access the network; the example keeps
all of them disabled. Pending delivery does not mean the recipient received it.
Repeating the same updates is idempotent. A restored state starts under a global
hold; old jobs and uncertain sends require explicit reconciliation even after
`release-recovery-hold` clears the global hold. Use private destinations outside
Git worktrees. Full and Git-only restore have [different guarantees](recovery.md).

## Operator URL and local files

`--url` captures one public URL through the normal durable pipeline without
polling or sending Telegram messages. Enabled metadata, translation, media and
narration providers still run; the configured independent Git archive is used
locally. Operator input commands never push a remote repository.

```sh
telegram-archive-bot --config /private/config.toml --url 'https://example.com/article'
telegram-archive-bot --config /private/config.toml summary '/private/article.md'
find /private/articles -type f -name '*.md' -print0 |
  telegram-archive-bot --config /private/config.toml summary --stdin0 --rename --auto-commit
```

`summary` requires an enabled model and accepts Markdown/text files as arguments
or newline-separated paths on stdin. `--stdin0` accepts NUL-separated paths,
including filenames containing newlines. Inputs are read-only bounded regular
files; symlinks and FIFOs are rejected. Originals and summaries are retained in
canonical CAS. Downloadable copies appear below
`state_dir/operator-results/JOB_ID/`. `--rename` names the retained copy from the
generated title; it never renames the input. Existing output with changed bytes
is reported as a conflict and preserved. `--auto-commit` uses only the configured
independent archive for local commits. Use `project` with explicitly enabled
push configuration for remote publication.

Telegram sending is disabled by default. `summary --send-telegram` requires an
enabled bot and exactly one allowed chat/user; it sends only this invocation's
persisted jobs. Uncertain deliveries remain unreconciled rather than being
blindly resent. Operator IDs are negative and do not advance polling cursors.
An identical input/configuration resumes the same job, including expired leases,
and reuses completed outputs. It does not reset exhausted retries, cancellation
or a recovery hold. Changed bytes or options produce a distinct request.

The JSON report lists each input, job, state, output paths and error. Partial or
failed results produce a nonzero exit while preserving other inputs' results.
The CLI claims only the selected jobs; a running service may also process them
through the same fenced leases. Capturing a URL is not proof that the origin
supplied complete content.

## Explicit live activation

Copy [config](../examples/config.toml) and [environment placeholders](../examples/bot.env.example)
to `$HOME/.config/telegram-archive-bot/`; use mode 0600 for secrets. Create the
state and separate archive directories with mode 0700 and configure actual
absolute paths. Set the real bot ID, both allowed chats and allowed users, and
explicit provider choices. Source URLs pass public-network policy; configured
internal model endpoints are a separate allowlisted configuration boundary.
FxEmbed fallback is opt-in and its provenance remains visible. Remote Git push
is opt-in. A private archive repository is recommended because export contains
source text; a public code repository is never an archive destination.

Run `check-config` and offline `doctor` first. Doctor sends no Telegram messages
and performs no provider HTTP probes; it checks local executables, codecs,
namespace capability, hashes, schema, integrity, headroom, enabled capabilities,
non-secret effective settings and queue/projection metrics. A successful doctor
is not provider authentication, remote model-weight attestation or client playback
acceptance. Keep a separate capability report and opt-in live evidence.

`run` starts a real poller when `telegram.enabled` is true. With only
`daily.enabled`, it runs the [bounded daily planner](daily-planner.md) without
Telegram polling or delivery; this planner leaf reports the unavailable timeline
provider explicitly. With `daily.capture.enabled`, it can also run the
[capture-only original hydration worker](daily-content-capture.md), consuming
persisted discoveries without Telegram/model/translation/TTS work. Before switching an existing bot, compare
an isolated offline replay, stop the old poller yourself, take a verified full
backup, and record the cursor. Only one installation may poll a bot. The local
service lock prevents duplicate instances using the same state directory; it
cannot discover an unrelated legacy installation. This project's installation
and tests do not activate or replace the old bot.

Install the user unit after configuring paths:

```sh
mkdir -p "$HOME/.config/systemd/user"
cp deploy/systemd/telegram-archive-bot.service "$HOME/.config/systemd/user/"
systemctl --user daemon-reload
# Explicit activation, only after the single-poller switch and backup:
systemctl --user enable --now telegram-archive-bot.service
journalctl --user -u telegram-archive-bot.service
```

The supplied unit uses the documented `$HOME/.local/bin` install location and
writable state/archive paths shown in the example. If these differ, use a unit
override for `ExecStart` and `ReadWritePaths`. `Restart=on-failure` waits five
seconds, with three starts per minute. SIGTERM/SIGINT stop admission and polling,
cancel work, and drain within the configured shutdown budget; supervised tools
kill/reap process groups. `KillMode=control-group` additionally lets systemd kill
remaining descendants at `TimeoutStopSec=30`. If increasing `shutdown_seconds`,
increase `TimeoutStopSec` accordingly. The unit needs unprivileged namespaces
for media and browser rendering; production code never changes host AppArmor/sysctl policy.

## Reload, upgrade and rollback

`systemctl --user reload telegram-archive-bot` sends SIGHUP. A replacement is
validated atomically; new jobs use its snapshot, running jobs keep theirs. Each
provider is constructed from its claimed job snapshot. State directory, bot ID,
Telegram enablement and resource-admission counts require a restart. Secrets are
references, never stored values. Environment-file changes and in-place secret
rotation require an explicit restart so all cached clients are replaced. Endpoint,
language, voice and non-secret limits can reload for new jobs; failures leave the
previous configuration active. X provider caches are cleared after successful
reload. Concurrency is conservative: one worker per phase plus a dedicated control
worker, with shared bounds for downloads, model requests, synthesis and FFmpeg.
Delivery uses one worker, never exceeding the configured delivery maximum.

Daily watchlist additions, disables and handle renames can reload. Established
watch-key identity changes are refused. New plans/attempts use the replacement
snapshot while in-flight attempts and historical day manifests retain theirs.
The daily planner checks reloads within one second and honors recovery holds.

For cross-job narration reuse, pin md2audio and its backend/local weights and set
`synthesis.deployment_revision`. Update it when that deployment changes; a Python
entry-point hash alone cannot detect package changes. Without this declaration,
completed audio remains reusable for recovery/delivery within its original job,
while new jobs synthesize independently. Do not update dependencies during a job.

Before upgrade, stop the service explicitly, create/verify a full backup, save the
old executable hashes, configuration and release version, then install the new
pinned candidate. Run doctor and isolated replay before an explicit restart.
Schema migrations are forward-only; an old executable rejects a newer schema.
Rollback therefore means stop the new instance, restore the pre-upgrade full
backup into a **new** directory, select the old binary/configuration, inspect the
recovery hold and uncertain deliveries, reconcile individually, then explicitly
restart. Never point an older binary at a migrated live database. The CLI recovery
exercise covers these primitives without touching a production service.

## Metrics, budgets and retention

Structured JSON logs contain job/phase/attempt IDs, elapsed processing time,
artifact hashes and delivery IDs/states. Default logs omit source bodies, prompts,
credentials, bot-token URLs, signed media URLs and raw provider errors. API
responses and producer provenance are retained only in private canonical storage.
`status`/periodic metrics report state counts, retries, retained bytes, observed
host cache reuse fraction and projection-event lag. Token/cost estimates are
explicitly unavailable when the provider does not report them. Query private
manifests for detailed measured durations and tool versions.

Defaults allow 300 seconds for a download, 1700 for an LLM phase, 1800 for TTS,
900 for finalization, 120 for a send, three attempts per phase and a six-hour
whole-job deadline. F18's combined producer/finalizer budget includes both stages;
the earlier absolute job deadline always wins. Waiting for a shared transformation
also consumes its attempt budget. Hymt's nested network/completeness retries are
zero; the persisted supervisor owns retry. Explicit Telegram 429 cooldowns survive
restart; ambiguous sends become unknown and need an acknowledged resend.

Every catalogued object is retained, including failed/unknown/pending delivery
artifacts. `gc` removes only abandoned unreferenced publication objects after a
grace period, coordinated with publishers and backups. There is no automatic
history eviction; monitor disk growth and keep at least the configured work budget
free. A projection lag or remote push failure is not loss of canonical source
storage. Archive editing/import is not supported; external managed-file edits
cause projection to fail closed for operator inspection.
