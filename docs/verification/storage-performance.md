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
