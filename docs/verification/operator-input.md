# Operator input evidence

Project-Spec: rust-telegram-archive-bot-v1. Spec-ID: F02, F03, F09, F11, F19, F20.
Tracking: [#49](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/49).

On 2026-10-24 the public CLI regression first failed because `summary` was not a
recognized command. The completed fixture uses a local synthetic model and the
actual CLI, canonical database, CAS and independent Git projection. It verifies
two source files, NUL-separated paths, summary and original outputs, unchanged
inputs, repeated-result reuse without model calls, and expired archive-lease
recovery. An unrelated queued job and polling cursor remain unchanged. Default
execution creates no delivery attempts.

The same fixture checks mixed missing/valid files, FIFO and symlink rejection,
private-network URL rejection, preserved edited-output conflicts, Unicode/newline
filenames, and SIGTERM during a model call. Cancellation leaves the subsequent
input unread and unadmitted. A long Chinese filename initially failed the
document-name boundary; the shared display-name function now bounds both bytes
and characters while retaining a short extension. The regression then passed.

A separate controlled Bot API fixture verifies scoped sending: only the selected
job is transmitted, while another job's expired sending attempt stays unchanged.
These tests do not send real Telegram messages or establish model quality.
Focused CLI and delivery checks and all-target Clippy passed. Full frozen gates,
independent review, installed execution and runtime activation have separate
receipts and must not be inferred from those focused checks.

One intermediate fixture failed because its archive was inside the public code
checkout. The existing overlap guard correctly rejected it; the fixture was
moved to an independent temporary directory. A preliminary compile failure and
all failed fixture logs were retained, not counted as successful tests.
