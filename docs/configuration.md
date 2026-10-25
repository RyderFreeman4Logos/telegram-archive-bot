# Configuration

Run `telegram-archive-bot example-config` for defaults. Save the result outside
the source checkout, at `$XDG_CONFIG_HOME/telegram-archive-bot/config.toml`
(otherwise `~/.config/telegram-archive-bot/config.toml`). `--config PATH` overrides
that location. TOML is the sole non-secret configuration authority; arbitrary
environment variables do not override it. Unknown fields fail validation.

Secrets are references, for example:

```toml
[telegram.token]
kind = "env"
name = "ARCHIVE_BOT_TOKEN"
```

Alternatively use `kind = "file"` and an absolute path to a regular mode-0600
file. Values are never serialized into snapshots. Both chat and user must be in
the explicit allowlists. Generation, capture and Telegram providers start disabled.
The Notion adapter is the deliberate default-on exception below. Enabled providers require
valid credentials before activation; model endpoints may deliberately use a
local service, unlike arbitrary submitted source URLs.

`check-config` checks syntax, policy and secret availability without network
requests. SIGHUP in the service will atomically validate and replace configuration
for new jobs. Existing jobs retain their serialized snapshot. State-directory
and bot-identity changes require a restart; a rejected reload retains the prior
configuration. Secret rotation occurs through the existing secret reference.

The opt-in [`daily` watchlist and local-date planner](daily-planner.md) defaults
to disabled and `America/Los_Angeles`. It has bounded backfill, automatic catch-up,
durable attempt/cursor snapshots and independent provider settings. Watch keys
remain stable across handle changes. Conflicting established identities reject
a reload. This planner leaf does not install a timeline provider or validate a
web session; planning/status never read its cookie/bearer references.

The independently opt-in [`daily.capture` worker](daily-content-capture.md)
hydrates committed activity sources with bounded context and documents. Its
default is disabled; each collection cycle pins its configuration and retains
previous revision associations. No user X account is needed for the existing
FxEmbed known-ID fallback.

The job supervisor owns retries: three attempts per phase, 1,700 seconds per LLM
attempt, six hours total execution including retry backoff. Queue time precedes
the absolute execution deadline. Other defaults are 300 seconds for downloads,
1,800 for synthesis, 900 for media processing and 120 for sends. Individual
attempts cannot exceed the job's remaining budget. Tool paths are absolute.

`sources.browser_enabled` opts into isolated JavaScript rendering after an
incomplete HTML fetch. Set `tools.browser` to a pinned native Chromium/Chrome
executable resolving under `/usr` or `/opt/google/chrome`; wrappers requiring a
user environment are unsuitable. Bubblewrap and its namespace support are
required. Doctor reports browser availability separately. The browser uses a
fresh profile and never imports host cookies or login sessions.

`sources.x_guest_enabled` optionally enriches an evidenced X Article preview when
the enabled official/FxEmbed provider has not supplied its body. It requires
`sources.x_guest_bearer`, a secret reference to the public web-client bearer,
and at least one of `x_enabled` or `x_fallback_enabled`. Ordinary posts and already
hydrated articles do not invoke it. This is separate from the official X API
bearer and does not establish access to private posts. The pinned guest query can
become unavailable when the upstream web API changes; denial remains explicit.
Restart the service after rotating this bearer file so the cached provider is
reconstructed. Guest tokens are reused for up to one hour and remain in memory
until the next fetch replaces an expired token or the provider is dropped. One
rejected token refresh is allowed within the same request byte and time budget.

Local TTS plus multiple speakers is rejected. Speakers are explicit resolved
voice assignments, not random choices at execution. Speech defaults to Chinese
voice notes, speed 1.0. Raw synthesis and finalized delivery are separate
derivations. All output-affecting configuration belongs in the job snapshot.

## Notion publication prerequisites (DA05 / #72)

Normal Cargo builds include `default = ["ntn"]`. Use
`cargo build --locked --no-default-features` for a build with no Notion transport.
An old configuration without a `[notion]` section resolves to `enabled = true`,
with no inferred executable, token or destination. `doctor.notion` reports
`pending_configuration` and specific missing prerequisites; local `ready` and
`check-config` do not fail just because Notion is unavailable. A minimal build
reports `compiled_out`. `enabled = false` returns `disabled` before Notion
filesystem/credential/probe work and suppresses every Notion subprocess.

```toml
[notion]
enabled = true
executable = "/absolute/path/to/ntn"
request_seconds = 30
operation_seconds = 300
payload_bytes = 450000
response_bytes = 2000000
record_bytes = 10000000
max_blocks = 10000
max_requests = 256
max_attempts = 3
retry_seconds = 30

[notion.token]
kind = "file"
name = "/absolute/private/notion-token"

[notion.destination]
kind = "page"
id = "YOUR-EXPLICIT-NOTION-PAGE-UUID"
```

For a data source, set `kind = "data_source"`, its explicit UUID and
`title_property = "Name"` (the actual title property name). No root is inferred
from a SOP page, the host's account, or an unrelated workspace. A token may also
use an explicit `kind = "env"` / `name` reference. Token files must be regular,
mode 0600 or stricter. References, not values, enter configuration snapshots.

The supported external executable is **ntn 0.22.10**, using API version
**2026-03-11**. Install and authorize it separately. The application does not
install/update tools, perform interactive login, open browsers or activate paid
providers. `ready` means local explicit prerequisites resolve; it does not claim
remote authorization. Version compatibility is checked at actual adapter use.
Every API subprocess has a private HOME/config/cache directory, a cleared
environment, explicit `NOTION_API_TOKEN`, `NOTION_KEYRING=0` and the fixed public
API endpoint. Ambient keychain/file login, proxies and other host credentials are
not inherited. Only the selected token is passed, and diagnostics never include
ntn output or its token value.

Request/response/record sizes, block counts, call counts and an absolute operation
deadline bound work. Per-request timeouts share that operation deadline. Values
over the configured bounds fail without truncating source text. `max_attempts`
and `retry_seconds` describe the durable retry policy for DA06; this adapter
does not automatically retry writes. A timeout/cancellation after create/append
dispatch returns `reconcile`, preserving a known page ID where available.

[DA06 (#73)](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/73)
adds automatic durable publication to `run`, including headless operation with
Telegram disabled. `SIGHUP` cancels/settles the previous Notion snapshot, and the
worker resumes when explicit local prerequisites resolve. Destination changes
bind only unbound/new work; old intent destinations remain immutable. Missing
prerequisites and disabled/compiled-out Notion never launch ntn. `doctor` remains
offline; its Notion readiness is separate from local archive readiness. See
[notion.md](notion.md) for queue states, bounded bootstrap, uncertain-write
reconciliation, operator controls and recovery. Live publication of this worker
requires a separate candidate-bound acceptance receipt.

## Reader corpus and authoritative SOP (DA09 / #76)

`reader.enabled` defaults true and derives local daily manifests independently of
Notion transport. Configure `[reader.sop]` `id`, `version`, optional `page_id` and
`page_url` with the deployment's authoritative reading policy. Either a valid ID
or URL configures the handoff; neither present is explicit pending configuration and prevents completed external-reader layers,
without blocking collection. The SOP never supplies a publication destination.
Bounds are `max_activities = 10000`, `max_windows = 1000` and
`export_bytes = 100000000`; excess work stays visibly pending with a build error.
Use `reader-status`, `reader-list`, `reader-export`, `reader-checkpoint` and
`reader-checkpoints`. SIGHUP atomically applies valid settings to new bounded
worker turns. See the [full five-layer contract](reader-handoffs.md).
