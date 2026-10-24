# Empty process-group grace measurement

Project-Spec: rust-telegram-archive-bot-v1; Spec-ID: F04, F17, F20.
Tracking: [#52](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/52).

On 2026-10-24 UTC, the public `project --limit 1` command was run four times
against independent full restores of the same synthetic canonical store.
The workload covered four event groups (six of 192 events), creating a separate
local Git archive with remote push disabled.

| Observation | Unconditional grace | Skip confirmed empty group |
| --- | ---: | ---: |
| Total elapsed seconds for four commands | 20.453 | 1.225 |
| Last exported event | 6 | 6 |
| Git fsck --strict exit | 0 | 0 |

Both exports have tree `0a7b8264f1c944e2ec2a45bd72b60d4ae25eb220`.
Commit IDs differ because each run records its own creation time. Control binary
SHA256: `99893646291312d3fc64b9c525a755a28f9871026ee88232a6993c090b10ae6f`.
Candidate binary SHA256:
`a18dbd0a07c6ae05f99cc5ad2386569e2fa99587dbc814b49acfba5b92fb93f9`.
The candidate is based on commit `315331d43cb8a28f9c6a9301358890c38301e7f7`.

The supervisor still signals, kills and reaps the process group. It skips only
the fixed 100 ms grace when a zero signal returns `ESRCH`; all other outcomes
keep the grace. The process fixtures cover cancellation and timeout with a
SIGTERM-resistant child, and a successful parent that leaves such a child.
They also check output/workspace bounds and error distinctions.

This is a small single-run comparison under shared host load, not full-corpus
export acceptance or a throughput guarantee. No remote push or physical SSD-wear
measurement was performed. Private receipts and the exact candidate patch remain
under `/var/tmp/archive-projection-performance-20261024/`.
