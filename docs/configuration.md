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
the explicit allowlists. All providers start disabled. Enabled providers require
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
