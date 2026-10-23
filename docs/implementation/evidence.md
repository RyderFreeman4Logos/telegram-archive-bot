# Executed checks

These are development receipts, not a completed F20 release acceptance report.
Only synthetic fixtures are stored publicly. Live credentials and user drafts
remain outside this repository.

| Change | Actual check | Result | Remaining scope |
|---|---|---|---|
| F01 bootstrap `24c7cb1` | GitHub CI [39910716158](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39910716158) | Passed scaffold formatting, lint, build | No feature behavior was present |
| F02 `1170578` | 3 configuration tests; fmt; clippy; example/check-config round trip; [CI](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39911041712) | Passed | Service reload and tool-version snapshots await integration |
| F03 storage foundation | `cargo test --locked`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo fmt` | 9 passed, including one child fixture entry point; lint clean | Backup integration and validated-media cataloguing await F17/F14 |
| F03 CLI | Debug binary `init`, `verify`, `gc` against an isolated fresh configured directory | schema 1, 0 retained blobs, 0 abandoned files | Not a production state migration |
| F04 | `cargo test --locked`; clippy all targets with warnings denied; formatting | 15 passed, including the child fixture; 6 source tests cover all six A/B/C orders, eight concurrent submissions, pinned revisions, counters/previews/deletion, aliases/queries/hash routes, missing stubs/cycles/budgets and schema-1 upgrade | Provider graph retrieval is integrated in F08/F09 |
| F05 | `cargo test --locked`; clippy all targets with warnings denied; formatting | 18 passed, including three ingress checks: UTF-16/caption/document bounds, transaction rollback/replay/auth/bot scope, and an HTTP fixture observing offsets 0 then 43 only after commit; synthetic token absent from transport diagnostics | Live polling and service command integration await runtime/delivery integration; no real poller started |
| F06 scheduler foundation | `cargo test --locked`; clippy all targets with warnings denied; formatting | 23 passed, including five job checks: stale lease fencing, persisted outputs/partial state, retry budgets across reopen, eight simultaneous claimants/absolute expiry, durable cancellation and shutdown drain | Actual ADK resume, resource admission and media/model cancellation are integrated in dependent features |
| F07 supervisor | `cargo test --locked`; clippy all targets with warnings denied; formatting | 29 passed; fake Python tool verifies literal metacharacters, unique workspaces, parent environment clearing, synthetic secret redaction, TERM-resistant descendant termination/reaping, stdout/stderr flood, total disk limit, timeout, nonzero exit, missing tool and path confinement | Real yt-dlp/md2audio/FFmpeg adapters and startup version snapshot integration follow; aggregate disk scans are sampled, not a filesystem quota |

The storage checks include eight simultaneous same-digest publications and
child-process exit at each durable publication checkpoint, followed by reopen,
integrity verification, orphan collection and republishing. Injected exceptions
also cover these checkpoints. This proves recovery from the tested process
failures, not physical power-loss behavior. Initial failures in fixture directory
permissions and the GC test notification channel were corrected before the
reported passing run.
