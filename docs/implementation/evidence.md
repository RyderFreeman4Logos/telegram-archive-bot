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

The storage checks include eight simultaneous same-digest publications and
child-process exit at each durable publication checkpoint, followed by reopen,
integrity verification, orphan collection and republishing. Injected exceptions
also cover these checkpoints. This proves recovery from the tested process
failures, not physical power-loss behavior. Initial failures in fixture directory
permissions and the GC test notification channel were corrected before the
reported passing run.
