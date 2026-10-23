# Durable Telegram ingress

The long-poll adapter requests a new offset only after the previous update's
SQLite transaction has committed. That transaction contains the bot-scoped
inbox identity, ordered normalized child requests, immutable configuration/tool
snapshot, and next offset. Replay is idempotent. Authorization requires both the
chat and user allowlists before parsing/scheduling work. Unsupported and rejected
updates retain only their ID and a fixed reason, then advance the cursor.

The private inbox retains bounded normalized requests, not the entire Telegram
payload. URL children retain first-seen order, deduplicated by provider identity.
Queries and hash routes remain meaningful. Text/caption entities use Telegram's
UTF-16 offsets, including text links. Markdown/text document requests retain a
file reference and checked declared size/type; downloads and UTF-8 validation
belong to the worker. Submitted names never determine a filesystem path.

Polling performs only bounded validation and short transactions. Source, model,
media and delivery work run independently. HTTP response size/time are bounded,
and transport errors hide URLs containing the bot token. The service loop and
command handling are integrated in the runtime/delivery features; the adapter
does not start a competing poller merely by constructing it.

Protocol reference: [Telegram getUpdates and MessageEntity](https://core.telegram.org/bots/api#getupdates).
