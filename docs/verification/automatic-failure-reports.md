# DA08 offline evidence and limits

Spec DA08 / [#75](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/75).
The writer starts from `b2f4829792f21c2676b470e88d61121e3f9eb4ed` in an isolated
worktree. The parent owns serial integration, independent review, exact-commit
canonical checks and publication. This document does not declare that those
later gates or a real GitHub write have passed.

Observed development failures were retained, including their failed exit status:

| Behavioral witness | Actual failed assertion | Original log SHA-256 |
| --- | --- | --- |
| Default reporter authorization absent on the base | JSON `Null` differs from `true` | `38b8664ef2eab200e56ab705a023e81b79e49debff1a620a7f83be33369f32e0` |
| Configured credential present in an exception | Strict fake gh rejected the public body; expected issue count 1, observed 0 | `87f1abf31da95fe297547c719fb68915fc611c9a1b881c78aca4b30bc62572db` |
| Aggregate valid URL/Unicode excerpts exceed body limit | Expected bounded report count 1, observed 0 | `62aab0da5deb00fe49d39b9aaaa62439beefa98f0f24200472c00a0e7d290279` |
| Retrying an invalid immutable Notion record before dispatch | Actual worker returned `failure event identity collision` | `015cd3158a1c815e1ac8654bb0c76541f72090f63b5e8900ac78c761e9f09929` |

An intermediate compile failure caused by a not-yet-connected configuration
snapshot method is retained as setup failure, not behavioral RED. The first
corrected ten-test reporter run and Notion settlement test passed before the
later bound/idempotence regressions were added; that historical result is not
substituted for final source-bound verification.

Final focused commands and exact source manifests are retained with the writer
receipt. Relevant public selectors are `cargo test --locked --test failure_reports`,
`cargo test --locked --test notion_worker`, affected configuration/source/recovery
tests, and default/no-default-feature Clippy and reporter tests. Fixtures use the
actual Tool subprocess boundary, private SQLite/CAS, real `run` process and
signals. They never invoke the installed gh or real GitHub APIs. Full backup
evidence is distinct from semantic Git-only recovery, which excludes reporter
operational records. No new live-provider, installed-runtime, client, or full-corpus
performance claim is made by this leaf.
