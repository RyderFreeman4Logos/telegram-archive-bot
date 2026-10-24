# SQLite connection reuse measurement

Project-Spec: rust-telegram-archive-bot-v1; Spec-ID: F02, F03, F20.
Tracking: [#47](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/47).

Measured on 2026-10-24 UTC using the public `replay` CLI, with 128 synthetic text
updates containing 32 distinct originals. Providers, Telegram sends and Git
projection were disabled. Each candidate used fresh private state, then replayed
the identical updates again. Input SHA256:
`95ecc2d76472d59817d61990a92f505443c16742eadacbf79442b536031a030d`.

The control is reply-command commit `255488a37da053895b9aff8af6aeef529c9bd522`
(binary SHA256 `09b85b8429c5a276534cabee42a5a89cf49ce9d9be1ef2d9d5cece9bd4ae9ba7`).
The candidate adds only the connection reuse implementation and its regression
test (patch SHA256 `93ad366155537e6d59319de6cc61cbc75d9dfea0ccf3d8aa363be9d8e49b2636`,
binary SHA256 `99893646291312d3fc64b9c525a755a28f9871026ee88232a6993c090b10ae6f`).

| Observation | Control | Reused connections |
| --- | ---: | ---: |
| First replay elapsed seconds | 71.468 | 18.010 |
| User + system CPU seconds | 11.925 | 2.996 |
| Last sampled process write_bytes | 894,607,360 | 241,549,312 |
| Last sampled cancelled_write_bytes | 138,444,800 | 0 |
| Peak RSS KiB | 33,048 | 33,044 |
| Repeat replay seconds | 0.101 | 0.041 |
| Retained blob bytes, before and after repeat | 81,804 | 81,804 |

Both runs exited successfully and passed SQLite integrity. Each retained 128
jobs, 32 sources, 32 revisions, 64 blobs and 128 outbox entries; repeating the
updates changed none of those counts or retained bytes. Regression tests also
cover rollback of an abandoned raw transaction, FULL synchronization, foreign
keys, clone lifetime and reopen integrity. Existing concurrent publication,
process-exit and backup/restore checks passed with connection reuse enabled.

These are single observations on the same host under other concurrent work,
not an isolated hardware benchmark or a latency guarantee. `/proc/PID/io` was
sampled every 20 ms; the last available sample can precede process exit.
`write_bytes` and `cancelled_write_bytes` are reported separately, and neither
is a measurement of physical SSD wear. The measurement does not cover network
retrieval, media conversion, live provider quality or Git export throughput.

Private durable receipts are retained under
`/var/tmp/archive-storage-performance-20261024/{reply_baseline,pooled}/receipt.json`.
An earlier RC1 control also passed (65.191 seconds), but it predates reply-command
changes and is not the isolated connection-reuse control used in the table.

## Source observation lookup cost

[Issue #61](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/61)
covers a separate large-archive lookup. The completed primary corpus has 18,347
sources and 43,523 observations. The existing SQLite plans scan all observations
for both a source's latest status and its preferred revision during Git restore.
Schema 12 adds one index on `observations(source_id)`. The capture quality lookup
also supplies its already-known source ID, preserving revision ownership and
the preference for complete content. Stored observations and originals do not
change; FULL synchronization and foreign-key checks remain enabled.

A bounded control copied those observation columns into an isolated in-memory
SQLite database and executed the exact status/restore lookup SQL for 800 sources.

| Lookup | Without index, seconds | With index, seconds |
| --- | ---: | ---: |
| Latest source status | 0.435 | 0.00146 |
| Preferred restore revision | 1.095 | 0.00298 |

Both result hashes are identical. Index creation took 0.0192 seconds in this
control. These measurements isolate query work; they do not measure disk writes,
public-command latency or complete Git restoration. Receipt SHA256:
`ba2064adba2012d1637005054d6a970926b0ba4766e82ca988f807afe514349e`.

The migration regression retains a schema-11 fixture with complete and later
partial evidence. It checks unchanged observation rows, preferred revision,
latest status and integrity, and indexed searches for both lookup shapes.
Its pre-change failure reaches the access-plan assertion (`SCAN observations`).
An earlier incorrect fixture count was corrected separately and is not counted
as a product failure. Installed migration, index storage cost and full-corpus
recovery require their own receipts. After opening schema 12, rolling back to a
schema-11 executable requires restoring a verified pre-upgrade backup.

## Bounded Git index updates

Project-Spec: `rust-telegram-archive-bot-v1`; Spec-ID: F16, F20.
[Issue #64](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/64)
batches up to 64 managed entries per `git update-index` invocation. It preserves
knowledge-event commit boundaries and staged/worktree conflict and recovery
checks. A real-Git regression observed 129 calls on the old helper and three on
the candidate, with exact entries, empty input, space/comma paths and unrelated
staging preserved.

A separate public `project --limit 1` comparison starts each case with 100,000
unrelated committed files and one unrelated staged file, then projects four
knowledge-event groups from the same synthetic canonical fixture. Baseline and
candidate alternate three times. Baseline source tree is
`b223c8ebf554263f2484e5719c53ccc30f4da96b` (RC2 `70dc6471`, identical to main
`56640473`); candidate source is `805f502ad7d55e7af24211ae6f3590e2b44abca6`.
The measured binary SHA256 values are respectively
`1811b1abd79dcd41db2e3b3dd5eb3ab5538cddc89b65899d05bf66f8eb32c549` and
`913f35a567f8b5682dbf19aec188cb0b22561b1a7f2e29c6224588ae0ece3cdd`.

| Pair | Baseline seconds | Batched seconds | Index-update calls before / after |
| --- | ---: | ---: | ---: |
| 1 | 2.964 | 2.011 | 40 / 8 |
| 2 | 2.964 | 2.061 | 40 / 8 |
| 3 | 2.913 | 2.011 | 40 / 8 |

All six cases have identical Git trees and index listings, preserve unrelated
staging, and pass Git fsck and canonical verification. Timings measure the four
projection commands, excluding fixture preparation and final verification.
The exact-source eight-command gate passed with 178 tests passed, zero failed
and two ignored. Benchmark receipt SHA256:
`156df4c8d5f139fa173cb21d2948ccede15b52994112cb2ece5b65db1207b2c8`.

These are sequential synthetic-index observations under ongoing export load,
without a confidence interval or causal timing attribution. They do not measure
the entire private corpus, physical SSD wear or provider latency. The pinned
full-corpus export remains a separate acceptance run with its original binary.
