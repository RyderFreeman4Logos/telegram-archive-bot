# Recoverable jobs

Each atomic claim increments a fencing generation and a persisted per-phase
attempt counter. Claims select queued/due retry work or an expired owner. The
first claim starts an absolute job deadline; reopening state never extends it.
Heartbeats and output commits require the current generation, live lease,
unexpired deadline and no cancellation request. Stale outputs cannot replace the
selected result. Phase outputs are immutable, and advancing to the next phase is
one transaction, so recovery skips completed expensive work.

Partial processing is persisted independently of the terminal flag and exposed
as `partial`. Source processing, Telegram outbox state and Git projection state
remain independent. Transient network/rate-limit/unavailable/timeout failures
use bounded backoff; other typed failures stop. Manual retry selects only the
current stopped phase and preserves both attempt and deadline budgets. An
expired/exhausted run needs an explicit new request/refresh.

Cancellation is durable, increments the fencing generation, and retains existing
outputs. Status/cancel/retry require the job's bot, chat and user scope. The async
lease supervisor propagates shutdown or lost ownership to a child cancellation
token and drains work for the configured shutdown interval. Model and process
adapters must use that token. Workflow mappings persist job/config/input identity
and the ADK checkpoint thread; graph execution is integrated in F10 rather than
implemented by the scheduler.

Resource admission, the complete service loop, Telegram command responses, ADK
execution and descendant cleanup are integrated by F07/F10/F15/F19; see
[operation](operations.md) for explicit activation and shutdown boundaries.
