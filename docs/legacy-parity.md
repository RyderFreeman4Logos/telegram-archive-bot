# Legacy behavior parity

Project-Spec: rust-telegram-archive-bot-v1
Tracking: [#44](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/44),
[#45](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/45)

The operator requested parity after the initial release. This inventory is based
on the read-only legacy source with SHA256
`76d0532daee72fcc611a52ec40e553c6cf4df8328cb82ee44c33ea4bd4bd3aee`.
No legacy source, private archive, credentials or Git history is imported.
An implemented path is not evidence that every live source can be recovered.

| Legacy behavior / entry | Rust path | Evidence and limits |
| --- | --- | --- |
| Multiple submitted URLs, `process_url` | `telegram::parse_requests`, durable child jobs | Frozen input accounting and source-bound corpus outcomes in release verification assets |
| Web article extraction, `scrape_with_trafilatura` | `Documents`, Readability + HTML-to-Markdown with marked fallback | Retained HTML triage and corrective replay; no claim of human semantic completeness |
| ThreadReader container, `scrape_threadreader` | Explicit `.threadreader-content` extraction | Explicit extraction fixture and per-input corpus results |
| X quotes/replies/reposts/articles, `scrape_twitter` | `XGraph`, official API and opt-in FxEmbed hydration | Final article eligibility audit and real fallback evidence; official bearer unavailable |
| Guest GraphQL article enrichment | Opt-in article-only guest provider after official/FxEmbed evidence | [Real provider capture and limitations](verification/x-guest.md); no eligible missing article bodies in the completed base corpus, so no incremental guest coverage claimed |
| JavaScript-rendered pages, `scrape_with_autocli` | Opt-in Chromium renderer with private profile and public-only proxy | Installed browser execution and retained correction results; no authenticated host profile |
| Original PDF even when text extraction fails, `scrape_pdf` | Validated original retention and independent delivery, Poppler extraction | Original-retention fixtures and corpus outcomes; configured size limits remain enforced |
| Audio/video links, `handle_audio_url` | Supervised yt-dlp + full audio validation, including Xiaoyuzhou routing | Full-decode fixtures, prior live acceptance and corpus outcomes; provider denials remain explicit |
| Uploaded Markdown/text narration, `handle_text_document` | Telegram file download, canonical source, md2audio, validated audio | Historical attachment fixtures passed; actual user-client reply-file roundtrip unobserved |
| Optional Chinese translation, `translate_markdown` | Pinned hymt bridge, separate immutable derivative and document | Controlled operation/output tests and prior live provider receipts; current client presentation unobserved |
| Title/summary and descriptive filename, `generate_article_enhancement` | ADK metadata, bounded filename, generated summary in delivery; dedicated `/summarize` output | Controlled operation/output tests and prior live provider receipts; current client presentation unobserved |
| Cached article resend | Durable outputs and `/get JOB` | Controlled cached resend and operation-scoping checks; actual historical client roundtrip unobserved |
| Separate Git export and optional push | Semantic projection and independent push cursor | Real local corpus export/restore and controlled remote fixtures are separate; real remote push untested |
| CLI single URL, `--url` | Scoped durable operator capture, no Telegram polling/sending | [Public CLI and installed execution](verification/operator-input.md) passed at their recorded source revisions |
| Batch summary of local files, `summary` | Read-only CAS ingestion, argument/stdin paths, renamed copies, optional Git and Telegram | [Repeat/recovery and preservation](verification/operator-input.md) fixtures and installed execution passed |
| Processing/failure feedback, `send_message` | New durable admission/failure notices | Durable outbox/retry fixtures passed; actual current-client feedback remains unobserved |
| Replies to initiating message, `reply_to` | New persisted origin and Bot API `reply_parameters` | JSON/multipart/restart fixtures passed; current user-client association unobserved |
| Configuration reload, `load_config` | Validated SIGHUP activation, immutable running-job snapshots | Immutable operation selection and reload/refresh fixtures passed |
| Restart/network retry and authorization | Service lock, persisted cursor, bounded job retries, chat + user allowlists | Source-bound recovery/authorization fixtures and separate runtime activation receipts |
| Requested historical reply commands | `/translate`, `/tts`, `/summarize`, `/start` | Source scoping and operation identity fixtures, exact-source gates, installation and deployment; user-client roundtrip unobserved |

The batch campaigns retain each run's exact executable digest, private canonical storage and
disabled Telegram sends/Git pushes. Model generation, translation and narration
are disabled in that capture-only campaign; media retrieval remains enabled.
It does not establish model quality or Telegram playback acceptance.

Reply sources use Telegram's same-chat `reply_to_message` object, including
supported files from before the service was installed. Cross-chat reply objects
and commands used as source text are rejected. Outgoing reply associations use
the incoming message ID rather than the update ID. See the
[Telegram Bot API](https://core.telegram.org/bots/api#replyparameters).

`/translate` returns a translated document, `/tts` returns validated narration,
and `/summarize` returns a summary document. Each uses the configured language,
voice and provider; a disabled capability produces a failure response.
Ordinary submissions retain the original document and send any translation as
a separate file. `/get JOB` resends stored results without repeating providers
or the original queue notice. Progress notices do not block final results when
the notice's send outcome is unknown.
