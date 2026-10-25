# Daily account/day planning (DA02 / #69)

The opt-in planner stores bounded account/day work in the existing SQLite
archive. It does not create another source store. DA01 accounts and immutable
source revisions remain authoritative. DA03 supplies [anonymous timeline enumeration](daily-timeline.md) through FxTwitter v2.
No X account, cookie, or paid API is required. Collection stays opt-in.

## Configuration and identity

All defaults preserve the existing Telegram workflow: `daily.enabled = false`
and an empty watchlist. A minimal isolated planner configuration is:

```toml
state_dir = "/private/daily-state"

[telegram]
enabled = false
[model]
enabled = false
[translation]
enabled = false
[synthesis]
enabled = false

[daily]
enabled = true
timezone = "America/Los_Angeles"
interval_seconds = 300
first_run_lookback_days = 7
reconciliation_days = 2
max_accounts = 100
max_days_per_request = 366
max_backfill_windows = 1000
max_windows_per_tick = 100
max_attempts = 3

[[daily.accounts]]
key = "example-account"
provider = "x"
handle = "example"
# user_id = "123456789" # Supply only a verified provider ID.
label = "Example"
enabled = true

[daily.provider]
kind = "fx_twitter"
max_pages = 5
max_scan_pages = 20
page_size = 40
```

Each account needs a unique operator-assigned `key` and at least one of `handle`
or numeric `user_id`. Keep the key when renaming a handle. Handles omit `@`;
labels are optional. Known provider IDs identify accounts independently of
handles. Without an ID, the key is an explicitly unresolved identity; a reused
handle does not prove that two accounts are the same. Changing an established
key's configured identity is refused, including adding/removing its configured
ID. It requires explicit reconciliation rather than silently rekeying history.
Removing or disabling a watch stops new admissions and claims, without deleting
its accounts, day plans, attempts or captured sources. In-flight attempts keep
their original snapshot.

The collection adapter can bind a verified provider ID under a fenced lease.
That immutable resolution records its evidence and rejects remaps or another
active watch resolving to the same account. Existing unresolved day IDs stay
unchanged; the adapter receives the resolved identity for DA01 activity records.

`kind = "x_web_session"` reserves the authenticated web-session settings for
DA03 and requires both `cookie` and `bearer` secret references, using the normal
`{ kind = "file", name = "/private/secret" }` or environment-reference syntax.
Planning, dry-run and status do not read those credentials or send requests.
DA02 validates reference presence, not session validity. Do not put credential
values in TOML, command arguments, page evidence or reason strings.

For a **500-account** watchlist, set `max_accounts = 500`; the hard limit is
1000 configured records. Seven days across 500 enabled accounts needs
`max_backfill_windows = 3500` or more (maximum 10000) for one explicit request.
Automatic catch-up is separately bounded by `max_windows_per_tick` (maximum
1000). For example, a budget of 100 visits and a 300-second interval admits
the initial backlog over multiple ticks. A durable round-robin cursor prevents
early accounts from monopolizing a small budget. Existing-day checks also
consume that budget. Increasing the account limit does not increase it
implicitly. Large explicit requests can instead be split into date ranges.

## Commands and automatic catch-up

```sh
telegram-archive-bot --config /private/daily.toml check-config
telegram-archive-bot --config /private/daily.toml daily-backfill \
  --from 2024-03-09 --through 2024-03-11 --dry-run
telegram-archive-bot --config /private/daily.toml daily-backfill \
  --from 2024-03-09 --through 2024-03-11
telegram-archive-bot --config /private/daily.toml daily-status --limit 100
telegram-archive-bot --config /private/daily.toml run
```

`--from` and `--through` are inclusive local dates; future local days are
refused. `max_days_per_request` bounds days (maximum 3660) and
`max_backfill_windows` bounds enabled accounts multiplied by days before any
writes. Dry-run creates no state directory and returns hypothetical plans; it
does not inspect existing database identity bindings. Actual backfill validates
bindings and commits the entire bounded request atomically. Repeating a range
reuses its account/timezone/date IDs and returns the stored immutable manifests,
including their original request range and configuration snapshot.

`daily-status` reports at most 1000 rows. Pass the last returned `id` as
`--after ID` to continue in ID order. It reports the global recovery hold and
`timeline_provider = "not_installed"` separately from window state. Queue
metrics also include `daily_windows`. Status can contain private handles and
cursors; keep its output private.

With daily enabled, `run` works with Telegram, models, translation and synthesis
disabled. It starts no Telegram poller or delivery worker in that mode. The
periodic planner executes immediately and at the configured interval. It adds
each new account's lookback through the open current day, then resumes its
durable next-date cursor after downtime. Cursors advance in the same transaction
as admissions, so interruption cannot silently skip an unplanned date.
`reconciliation_days` independently revisits recent local dates (zero disables
it); its own durable cursor progresses even with a one-window tick budget.
Completed windows become eligible again when their reconciliation interval is
due, while historical successful attempts remain retained.

Valid SIGHUP reloads replace the ConfigManager snapshot atomically. The planner
observes changes on its next one-second wakeup; new admissions and collection
attempts use that snapshot. Immutable day manifests keep the original admission
snapshot, and in-flight leases keep their attempt snapshot. Invalid TOML, unknown
fields, invalid limits or conflicting established identities retain the prior
configuration. Telegram enablement, state directory and existing runtime
admission-count restrictions still require a restart. Disabling daily stops new
admissions without changing old records. SIGTERM/SIGINT uses the existing
bounded shutdown path.

## Dates, progress and recovery

Each day stores `[utc_start, utc_end)` from consecutive local midnights in the
configured IANA timezone. Adjacent days share exactly one boundary. For Los
Angeles, 2024-03-10 lasts 23 hours and 2024-11-03 lasts 25 hours. A nonexistent
or ambiguous midnight is refused explicitly, including skipped civil dates;
the planner never guesses an offset or changes process-global `TZ`.

| State | Meaning |
| --- | --- |
| `planned` | Admitted, with collection still unfinished |
| `collecting` | One fenced attempt holds an unexpired lease |
| `partial` | A bounded attempt left unfinished work and a resumable cursor |
| `blocked` | An explicit prerequisite or progress barrier prevents collection |
| `failed` | Failure is recorded, including an exhausted retry budget |
| `complete` | Adapter asserted actual closed-window coverage with no continuation |
| `open_current_day` | The day is still open; it cannot be final coverage |

With the provider disabled, the automatic loop reports `timeline_provider_not_installed`
with zero collection attempts. The anonymous provider records raw response hashes,
continuation and uncertainty; provider absence is never an empty timeline or
completed coverage. See the transport document for actual live evidence and limits.

Claims pin configuration, watch, attempt and generation. Checkpoints atomically
persist the cursor and an immutable per-page evidence receipt, bounded by
`max_pages` and the input-byte budget (at most 1 MiB per evidence payload).
The adapter can reference retained raw-page CAS hashes in those receipts.
Repeated/cyclic cursors and changed lease snapshots are refused. Expired leases
resume the same window/cursor under a new generation; stale writers are fenced.
`max_attempts` bounds consecutive failed/interrupted attempts, not successful
page-progress continuations. Successful progress resets that failure counter;
exhausted windows remain visible and do not starve other work. The adapter must
provide honest coverage evidence; setting a state alone does not enumerate a
timeline. Current-day completion is always converted to open state.

After correcting a credential/provider prerequisite, explicitly run
`telegram-archive-bot --config /private/daily.toml daily-retry WINDOW_ID` for a
failed or blocked window. It resets the consecutive failure budget and retry
time, fences old generations, and retains the cursor, immutable manifest and
all attempt/page receipts. It requires an enabled watch and refuses collecting
or completed windows. It does not collect immediately, bypass a recovery hold,
or reset every historical failure on an unrelated config edit. Credential file
rotation alone does not reset exhausted windows.

Schema 14 adds operational planner tables without replacing DA01/source data.
Verified full backups retain manifests, config references, cursors, attempts,
page receipts, bindings and admission cursors. Restore interrupts active
collections, fences generations, preserves their cursor, and installs the global
recovery hold. The hold prevents automatic admissions and claims until explicitly
released; it does not perform provider requests or sends. Older supported
databases acquire empty planner tables. Semantic Git recovery retains DA01
activity/source history but **does not reconstruct operational daily plans or
collection checkpoints**; use a full backup for scheduling continuity.
