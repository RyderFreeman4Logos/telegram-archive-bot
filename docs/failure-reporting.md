# Automatic failure reports

**Spec ID: DA08** — [#75](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/75),
dependent on DA01, DA03 and DA06. This independent queue supplements local
diagnostics and daily coverage accounting. It does not turn an unavailable source
into a successful capture.

`run` automatically starts the reporter, including with Telegram and models off.
Reporting defaults to enabled for the explicitly authorized repository
`RyderFreeman4Logos/telegram-archive-bot`. The executable and authentication remain
explicit prerequisites; the repository is never inferred from the working
directory. Example configuration:

```toml
[failure_reporting]
enabled = true
repository = "RyderFreeman4Logos/telegram-archive-bot"
executable = "/absolute/path/to/gh"
token = { kind = "file", name = "/private/path/gh-token" }
threshold = 3
debounce_seconds = 300
update_seconds = 3600
retry_seconds = 60
timeout_seconds = 30
labels = []
```

The token file must be private (0600 or stricter). An explicit `env` secret
reference is also supported. The subprocess receives only the chosen token,
`GH_HOST=github.com`, disabled prompts/update notices, and an empty private gh
configuration directory. It uses the existing bounded Tool supervisor with
process-group cancellation and reaping. There is no automatic installation,
login, browser authentication, inherited gh session or inherited proxy.
Disabled reporting starts no gh process and performs no gh/tool/auth readiness
probe. Missing executable/auth is `pending_configuration`; a failed remote read
is retryable. Neither blocks canonical intake or Notion publication. Reporter
errors are safe local categories and cannot recursively create incidents.

## Producers and privacy

`Store::record_failure(&FailureEvent)` records an event transactionally without
subprocesses. The crate-internal `failure_reporting::record_in_tx` is the same
boundary for a collector's existing settlement transaction. Notion validation,
failure settlement and verified recovery already use this transaction boundary.
Timeline and content collectors must call it at their own durable settlement
points during serial integration; this isolated leaf does not invent observations
for those adapters.

- `event_id` is a stable observation/attempt identity. Replaying identical input
  is a no-op; conflicting data under that ID is rejected.
- `stream_id` is stable across retries of one operation/source. A recovered
  stream cannot clear another unresolved source's defect.
- The normalized fingerprint includes subsystem, protocol provider/operation,
  error class, application version and provider schema. Protocol tags must never
  contain account names, URLs or credentials.
- `window_id` is private and hashed for distinct affected-window counts. It is
  not part of the fingerprint. Source URLs do not determine issue identity.
- `Source::PublicUrl` is an explicit disclosure declaration for a public canonical
  tweet/article/PDF URL. Supply a safe actionable `public_excerpt` and any known
  secret/private strings in `redactions`. Do not pass an unfiltered provider body
  as a public exception. Authorization query parameters, signed-download tokens,
  user information, fragments and tracking query fields are removed; ordinary
  source-identity parameters are preserved. Embedded exception URLs are omitted
  in favor of the separately declared source. Credential/cookie lines, known
  secrets and the configured dispatch token are redacted.
- `Source::Private` always produces a stable hashed incident reference and a
  typed safe failure message. Its source, account lists, protected content and
  exact `private_details` stay in private SQLite/full backups. Notion records use
  this conservative boundary even if some original URLs happen to be public.

The producer owns the public/private classification; lexical filtering is not a
proof that arbitrary protected text is public. Public reports are English and
include the operation, versions, counts, first/last observations, current state,
source diagnostics, reproduction, expected and observed behavior. They contain
at most six latest observations and 32 KiB, with an explicit omission notice if
sample excerpts hit the byte bound. Aggregate counts retain all observations.
Each event is bounded to 96 KiB and private diagnostic text to 64 KiB.

`Transport`, `InvalidResponse`, `Extraction`, `Integrity` and `Internal` are
actionable after the threshold and debounce. Deleted/private/unavailable content,
rate limits, missing optional configuration, remote human conflicts and
cancellation remain separately classified gaps. They do not generate one issue
per missing source. A recovered actionable stream before eligibility creates no
issue. Recovery events must identify the matching defect class/provider schema;
they are not a claim that every historical version was fixed.

## Remote effects and recovery

A private per-store OS lock serializes local workers. Repository-specific queues
bind at most 64 existing incidents per pass through durable keyset cursors; changing
the configured repository never rewrites old report targets. SIGHUP cancels and
settles the old reporting operation before adopting the new snapshot.

Before a create, the worker searches all open/closed issues for the exact
fingerprint marker and consults its local receipt. Matching open issues are reused.
The explicit recurrence policy is to reopen the same closed issue when a new
active recurrence meets the threshold/debounce, then append the aggregate update.
Normal updates obey `update_seconds`; no issue is automatically closed. Multiple
workers sharing the canonical state share one queue and one tracked issue.
Independent databases do not provide a distributed exactly-once guarantee.

Every create/comment/reopen intent, immutable expected body, sequence and attempt
is committed before dispatch. Create/comment use structured argv and a 0600
`--body-file`. A successful CLI exit or echoed URL is insufficient: the worker
reads the actual issue/comment body and verifies its exact expected content,
configured repository, issue number and fingerprint. Human edits are never
overwritten. Ambiguous markers, malformed results, truncated output and mismatches
remain visible failures. A remote read is bounded to 1 MiB; an operation has one
shared timeout across its child commands.

After an uncertain create/comment, only marker/body reconciliation is allowed;
an empty search is not permission to write again. This includes timeout, process
death and cancellation. Reopen is idempotent and may be retried after readback.
The original failed attempt outcome is retained separately from a later verified
timestamp. Persistent absence or human edits may require operator investigation;
there is deliberately no command that asserts an uncertain create did not happen.
The reporter does not claim exactly-once GitHub delivery.

`failure-report-status --limit 20` (maximum 100), `status.failure_reporting` and
`doctor.failure_reporting` expose safe counts, identities, state and readiness;
they do not print source bodies or credentials. Doctor performs no GitHub request.
Full backup retains events, queues and attempts. Restore sets the global hold and
treats unfinished or not-yet-bound historical incidents as potentially published.
After the operator releases the hold, those incidents reconcile first. Semantic
Git exports exclude this private operational accounting and its authentication.

## Integration and acceptance boundary

Migration 018 belongs to this leaf. The isolated base is schema 015; independently
owned 016 and 017 must be applied in order during parent integration before 018.
Do not deploy this isolated migration sequence. The historical-schema fixture
constructs actual 001–015 DDL and proves canonical observations/Notion intents
survive the additive reporter migration.

Offline tests invoke a strict synthetic gh process and the real service child,
exercise signals, accepted-but-lost replies, remote readback, multiple workers,
privacy, debounce, cadence, closed recurrence, backup/restore and Notion settlement.
No production GitHub issue was created by these tests. A real GitHub test and the
timeline/capture producer integration remain separate acceptance work. Ordinary
configured production incidents are already authorized and do not need a
per-failure confirmation; a deliberately created live test is separately scoped.
