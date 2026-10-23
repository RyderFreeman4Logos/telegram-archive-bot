# Durable delivery and commands (F15, issue #15)

The host queues one immutable ordered delivery batch for a live job lease.
Audio payloads name a ready F14 artifact, never a raw producer path. Documents
name a retained CAS PDF/UTF-8 object with a validated filename/MIME. Plain
messages are bounded to 4096 UTF-16 units; captions to 1024. No parse mode is
used, so Markdown/HTML metacharacters remain literal. The optional caption
helper clips at a Unicode boundary and adds an ellipsis; full documents are
not clipped. PDF and text/Markdown delivery use the same durable queue and
attempt discipline without pretending that they are audio artifacts.

Schema 7 retains pending, sending, confirmed, retryable, terminal, unknown and
paused outbox states, plus every numbered attempt and private bounded response
blob. A logical `(job, ordinal)` has one immutable payload. Earlier unconfirmed
segments block later ones. Separate workers can process different jobs; a
second worker must recheck eligibility after preflight and cannot claim an
active attempt. File reads occur outside database write transactions, preventing
inversion with the CAS publication lock. A focused regression holds that lock
while proving recovery commits and a separate database writer can proceed.

An attempt is committed before HTTP transmission. Upload requests carry real
MIME, duration rounded from F14's measured milliseconds and the selected audio
or voice presentation. A success receipt requires the expected chat, positive
message ID, correct media field, nonempty `file_id`/`file_unique_id`, and duration
within one second of that request. A supplied MIME must agree. MIME is optional
in the upstream response schema and is retained as absent when not supplied.
A malformed success stays unknown with its response available for diagnosis;
it cannot populate reusable media IDs. Confirmed means a validated API receipt,
not audible completeness or client seeking/speed acceptance.

Media IDs are indexed by bot, exact representation and presentation type.
Document MIME/name also participate in the representation key. A cached-ID
request explicitly rejected with HTTP/API code 400 invalidates that ID and
queues upload fallback as a separately recorded attempt; this rejection does
not consume the upload retry allowance. No producer runs during this path.
Explicit server rejections have bounded retries. `retry_after` establishes a
persisted bot-wide cooldown, including other messages and manual retries.

A connection failure known to precede transmission is retryable. Other transport
errors, cancellation after an attempt begins, invalid/missing responses and
deadlines are conservatively unknown. Sending leases expire into unknown;
legacy sending rows without a lease do too. Recovery never automatically
replays them. A late response can attach diagnostic evidence to its attempt
but cannot overwrite a recovered queue decision. There is no exactly-once
external-send claim. All catalogued CAS data remains retained, including
pending/retryable/unknown inputs; reference-aware retention work belongs to F18.

Authorized commands are scoped to bot **and** chat **and** user:

- `/help` lists commands; `/status [JOB]` reports bounded recent jobs or a job's
  status and first ten delivery records, with the total count.
- `/cancel JOB` stops processing and unsent deliveries; an already transmitted
  send can still finish. `/retry JOB` resumes only its stopped current phase
  within its existing attempt/deadline budget.
- `/retry delivery OUTBOX` grants another bounded delivery retry allowance;
  it cannot retry an unknown send or bypass a persisted rate-limit cooldown.
- `/get JOB` queues saved deliveries and a response, without model/TTS calls.
- `/refresh JOB` creates a separate forced source run under the command's current
  configuration snapshot. It preserves the previous job and its outputs.
- `/resend OUTBOX --accept-duplicate-risk` explicitly permits a possible duplicate.

Command control effects, new refresh jobs, reply deliveries and command
completion share one lease-fenced transaction. A failed reply selection rolls
back the action. Saved files are preflighted before the write transaction and
the immutable payloads are checked again inside it. Job fences also bind the
stored config ID, normalized snapshot, request and force flag. Replacing a
lease with another existing job's valid config cannot authorize publication.

The local fixture server uses synthetic tokens and real finalized sine-wave
audio. One test kills a child process **after** the fixture accepted a send and
returned its receipt, then proves restart recovery sends nothing further.
Additional tests cover raw-artifact rejection, MIME/duration receipts, cached-ID
fallback, bot/presentation isolation, rate guidance, explicit server errors,
missing identifiers, malformed JSON, disconnects, timeouts, cancellation,
concurrent claimers, corruption, legacy migration and command rollback/scope.
These checks certify neither the public Telegram service nor client controls.
The service dispatcher and real run evidence are integrated in F19/F20.

The boundary follows the [Telegram Bot API](https://core.telegram.org/bots/api#sendaudio),
including [voice messages](https://core.telegram.org/bots/api#sendvoice) and
[retry guidance](https://core.telegram.org/bots/api#responseparameters).
