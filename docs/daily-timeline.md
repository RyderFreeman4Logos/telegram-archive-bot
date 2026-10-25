# Daily timeline discovery (DA03 / #70)

Use `[daily.provider] kind = "fx_twitter"` for public, anonymous FxEmbed v2 account
lookup and paginated statuses, including replies. No X login, API key, or cookie
is read or sent by this path. The default remains disabled. `daily-status`
reports configured readiness, not a claim of upstream availability.

The fixed HTTPS API is `https://api.fxtwitter.com/2/profile/{handle}` and
`/2/profile/id:{user_id}/statuses?with_replies=1&count=...&cursor=...`.
The client requests neither translation (`lang`) nor source-time filtering
(`since`), because source creation time does not establish repost time.
Transport uses the existing public-address, redirect, byte, timeout and
cancellation limits. No new dependency or secondary source database is added.

## Coverage and storage

Every response, including failures, is retained once by content hash. HTTP/API
errors cannot turn into an empty successful day. Handle lookup binds a strong
ID under the current fenced lease. Each page commits activities before its
immutable cursor checkpoint; reopening validates the previous blob and replays
one overlap page. A changed overlap page records a gap. Cursor cycles, unknown
rows, missing termination and unavailable timestamps stay visible.

Original posts, replies and quotes retain provider event ID and creation time.
FxEmbed exposes reposted content and actor, but omits the repost event ID and
its timestamp. Reposts therefore retain unknown activity time and a scoped
window/page occurrence identity; the original source ID/time remain separate.
They are never silently placed into a day using the original post's time.
Unknown-time activity keeps coverage incomplete. Conversation context from a
different actor is not counted as the watched account's activity. Quotes and
reply ancestors retain typed source edges for the separate capture worker.

The endpoint may omit upstream deleted, protected or unavailable material.
A provider-visible end is not proof of inaccessible historical X content.
Only explicit provider termination with no observed gaps permits completion;
an open current day never becomes final. An old or pinned post alone never
terminates a scan. Content hydration, Notion publication and delivery are
separate durable stages; discovery creates no jobs/outbox messages or model/TTS
requests. Raw responses and activity observations share the canonical archive.

`max_pages` bounds checkpointed pages per claim. `max_scan_pages` (default 20,
range 1–512) bounds the whole persisted scan across claims. Reaching it records
`provider_scan_budget` and retains the cursor, not completion. Increase the
limit and use `daily-retry WINDOW_ID` to continue. This bounds large watchlists
without re-fetching entire histories forever. `page_size` is 1–100; upstream
conversation context can make actual rows exceed that requested size. Aggregate cursor checkpoints are bounded to 256 KiB, including worst-case JSON escaping of two 16 KiB
provider cursors and up to 512 seen hashes. One storage-owned verification proof
under the CAS fence is reused across all activities on a page; public single
capture paths still verify independently. Each
raw page remains byte bounded and parsing is capped at 10,000 entries. Retry
attempts/backoff are bounded; numeric Retry-After is persisted without sleeping
inside the SQLite transaction. Other accounts can proceed while one is blocked.
Changing provider mid-scan is blocked to avoid sending incompatible cursors.

## Evidence and contract attribution

API contract: [FxEmbed documentation](https://docs.fxembed.com/api/introduction/)
and [FxEmbed source at 76e3726b](https://github.com/FxEmbed/FxEmbed/tree/76e3726b4a890a8752a12627bc9885323f413e09),
including its OpenAPI profile/statuses schema and `userStatuses.ts`.
Bounded anonymous probes on 2026-10-25 returned HTTP/API 200 for NASA profile,
23 NASA status rows, 25 XDevelopers rows and a cursor-followed XDevelopers second
page. No authentication was supplied. These are endpoint availability receipts,
not exhaustive account/day coverage or deployment acceptance. Synthetic tests
separately exercise concrete HTTP headers, cursor encoding, strong lookup,
SQLite reopen/overlap, interruption, rate limits, and unknown repost timing.

The optional `x_web_session` path uses explicit secret references and pinned
GraphQL operations/features from [twscrape 63f61a5](https://github.com/vladkens/twscrape/tree/63f61a518e576ea5c132f1156c03dda0520d76c1).
It is not required by the anonymous deployment and has no live authenticated
acceptance claim. It remains subject to upstream operation changes.

The concrete Rust anonymous client was also exercised against XDevelopers in an
isolated SQLite archive on 2026-10-25. Two bounded attempts across a store reopen
committed two pages; the second used a nonempty continuation cursor. Readback
found 20 activities, 28 observations, one strong account binding and zero jobs
or outbox records. The window correctly remained partial because repost times
and a changed overlap page prevented complete coverage. The final run exited
0 in 6.92 seconds including its incremental build. This verifies live discovery
and persistence only; it is not an exhaustive daily capture/publication claim.
A preceding run exposed volatile-response overlap accounting and is retained
as failed behavioral evidence; its regression and corrected pagination passed.
