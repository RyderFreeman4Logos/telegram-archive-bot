# Legacy behavior parity

Project-Spec: rust-telegram-archive-bot-v1
Tracking: [#44](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/44),
[#45](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/45)

The operator requested parity after the initial release. This inventory is based
on the read-only legacy source with SHA256
`76d0532daee72fcc611a52ec40e553c6cf4df8328cb82ee44c33ea4bd4bd3aee`.
No legacy source, private archive, credentials or Git history is imported.
An implemented path is not evidence that every live source can be recovered.

| Legacy behavior / entry | Rust path | Remaining acceptance |
| --- | --- | --- |
| Multiple submitted URLs, `process_url` | `telegram::parse_requests`, durable child jobs | Full corpus replay |
| Web article extraction, `scrape_with_trafilatura` | `Documents`, Readability + HTML-to-Markdown with marked fallback | Compare real retained text and partial captures |
| ThreadReader container, `scrape_threadreader` | Explicit `.threadreader-content` extraction | Corpus outcomes |
| X quotes/replies/reposts/articles, `scrape_twitter` | `XGraph`, official API and opt-in FxEmbed hydration | Compare article-body coverage; official bearer unavailable |
| Guest GraphQL article enrichment | FxEmbed currently supplies hydrated article bodies | Audit any remaining preview-only cases and the missing fallback |
| JavaScript-rendered pages, `scrape_with_autocli` | Opt-in Chromium renderer with private profile and public-only proxy | Installed browser execution and real corpus reruns; no authenticated host profile |
| Original PDF even when text extraction fails, `scrape_pdf` | Validated original retention and independent delivery, Poppler extraction | Live corpus and matching configured size limits |
| Audio/video links, `handle_audio_url` | Supervised yt-dlp + full audio validation, including Xiaoyuzhou routing | Corpus/live results |
| Uploaded Markdown/text narration, `handle_text_document` | Telegram file download, canonical source, md2audio, validated audio | Historical reply-file live test |
| Optional Chinese translation, `translate_markdown` | Pinned hymt bridge, separate immutable derivative and document | Live presentation |
| Title/summary and descriptive filename, `generate_article_enhancement` | ADK metadata, bounded filename, generated summary in delivery; dedicated `/summarize` output | Live presentation |
| Cached article resend | Durable outputs and `/get JOB` | Verify reply-based use of saved files without unrelated transforms |
| Separate Git export and optional push | Semantic projection and independent push cursor | Local fixture passed; real remote push remains untested |
| CLI single URL, `--url` | Scoped durable operator capture, no Telegram polling/sending | Public CLI and installed execution |
| Batch summary of local files, `summary` | Read-only CAS ingestion, argument/stdin paths, renamed copies, optional Git and Telegram | Repeat/recovery, input/output preservation and installed execution |
| Processing/failure feedback, `send_message` | New durable admission/failure notices | Focused outbox and retry checks, live delivery |
| Replies to initiating message, `reply_to` | New persisted origin and Bot API `reply_parameters` | JSON/multipart/restart checks and live client |
| Configuration reload, `load_config` | Validated SIGHUP activation, immutable running-job snapshots | Preserve operation selection through `/refresh` |
| Restart/network retry and authorization | Service lock, persisted cursor, bounded job retries, chat + user allowlists | Existing coverage plus deployed service evidence |
| Requested historical reply commands | `/translate`, `/tts`, `/summarize`, `/start` | Source scoping, operation identity, full gates and deployment |

The batch campaign uses the released executable, private canonical storage and
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
