# Executed checks

These are development receipts, not a completed F20 release acceptance report.
Only synthetic fixtures are stored publicly. Live credentials and user drafts
remain outside this repository.

| Change | Actual check | Result | Remaining scope |
|---|---|---|---|
| F01 bootstrap `24c7cb1` | GitHub CI [39910716158](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39910716158) | Passed scaffold formatting, lint, build | No feature behavior was present |
| F02 `1170578` | 3 configuration tests; fmt; clippy; example/check-config round trip; [CI](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39911041712) | Passed | Service reload and tool-version snapshots await integration |
| F03 storage foundation | `cargo test --locked`; `cargo clippy --locked --all-targets -- -D warnings`; `cargo fmt` | 9 passed, including one child fixture entry point; lint clean | Backup integration and validated-media cataloguing await F17/F14 |
| F03 CLI | Debug binary `init`, `verify`, `gc` against an isolated fresh configured directory | schema 1, 0 retained blobs, 0 abandoned files | Not a production state migration |
| F04 | `cargo test --locked`; clippy all targets with warnings denied; formatting | 15 passed, including the child fixture; 6 source tests cover all six A/B/C orders, eight concurrent submissions, pinned revisions, counters/previews/deletion, aliases/queries/hash routes, missing stubs/cycles/budgets and schema-1 upgrade | Provider graph retrieval is integrated in F08/F09 |
| F05 | `cargo test --locked`; clippy all targets with warnings denied; formatting | 18 passed, including three ingress checks: UTF-16/caption/document bounds, transaction rollback/replay/auth/bot scope, and an HTTP fixture observing offsets 0 then 43 only after commit; synthetic token absent from transport diagnostics | Live polling and service command integration await runtime/delivery integration; no real poller started |
| F06 scheduler foundation | `cargo test --locked`; clippy all targets with warnings denied; formatting | 23 passed, including five job checks: stale lease fencing, persisted outputs/partial state, retry budgets across reopen, eight simultaneous claimants/absolute expiry, durable cancellation and shutdown drain | Actual ADK resume, resource admission and media/model cancellation are integrated in dependent features |
| F07 supervisor | `cargo test --locked`; clippy all targets with warnings denied; formatting | 29 passed; fake Python tool verifies literal metacharacters, unique workspaces, parent environment clearing, synthetic secret redaction, TERM-resistant descendant termination/reaping, stdout/stderr flood, total disk limit, timeout, nonzero exit, missing tool and path confinement | Real yt-dlp/md2audio/FFmpeg adapters and startup version snapshot integration follow; aggregate disk scans are sampled, not a filesystem quota |
| F08 | `cargo test --locked`; clippy all targets with warnings denied; formatting | 35 passed; X fixtures cover concurrent alias/cycle traversal with one fetch per identity, fallback provenance, full-content protection from excerpts, articles/repost references/tombstones, node/depth/reply/byte limits, cancellation and a silent-provider deadline; network unit checks reject private/special addresses | Public/official provider live availability remains untested; worker integration must fence final canonical selection |

| F09 | `cargo test --locked`; all-target clippy with warnings denied; formatting | 42 passed, including four document fixtures and three additional HTTP checks: HTML/ThreadReader, Unicode/UTF-16/declared charset, concurrent same-name PDFs, real Poppler validation/extraction, failing/empty/invalid PDF outcomes, private redirects, credential-origin limits, mixed DNS, bounded streamed bodies, silent-body deadline and cancellation | No real webpage, Telegram attachment download, OCR, or client UI check is claimed; worker integration remains pending |

| F10 | `cargo test --locked`; all-target clippy with warnings denied; formatting | 51 passed, including nine ADK checks (one child entry point): schema/refusal/truncation failures, UTF-8 single-paragraph splitting, terminal response replacement, exact checkpoint frontier resume, abrupt exit before checkpoint with derivative reuse, silent/slow-stream cancellation/deadline, metadata worker transition, snapshot mutation rejection, and a real local 401 fixture proving one send and redacted diagnostics | Live model semantic acceptance and full daemon/source/media integration remain pending |

F09 PR CI [39984359507](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39984359507)
passed; PR #30 was merged.

F08 push CI [39914669239](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39914669239)
passed. Its subsequent PR check [39982782539](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39982782539)
initially timed out (exit 124) while downloading system packages, before code
checks. The retry passed. The initial infrastructure failure is retained as a failed attempt.

The storage checks include eight simultaneous same-digest publications and
child-process exit at each durable publication checkpoint, followed by reopen,
integrity verification, orphan collection and republishing. Injected exceptions
also cover these checkpoints. This proves recovery from the tested process
failures, not physical power-loss behavior. Initial failures in fixture directory
permissions and the GC test notification channel were corrected before the
reported passing run.

During F09 verification, the process fixture intermittently lost its entire
`/tmp` root. A traced failure showed removal outside the traced test process
tree, followed by `ENOENT` at process launch or directory checks. Process
fixtures now use private temporary roots under the repository's ignored target
directory. Production directory-loss errors remain failures. The final full
suite passed, and 30 additional complete process suites passed with the isolated
fixture roots.

F10 PR [#31](https://github.com/RyderFreeman4Logos/telegram-archive-bot/pull/31)
merged after [39987115986](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39987115986)
passed. The original push check first timed out during apt package installation;
its rerun was superseded/cancelled by the successful PR check, before any claim of
code-check success from that failed attempt.

| F11 | Build pinned Rust bridge; host `cargo test --locked`; bridge `cargo test --locked --manifest-path integrations/hymt-bridge/Cargo.toml`; formatting and all-target Clippy for both workspaces | 58 host checks and 1 bridge check passed. Six integrated tests run the actual bridge against local HTTP: mixed Markdown/code/Unicode and multiple segments, verified cache reuse, changed profile invalidation, unverified bypass, detached configuration after file mutation, empty translated payload despite protected URL, retained-original truncation, failing later segment, silent cancellation/timeout, omitted sampler fields and ADK phase transition. A unit check rejects tampered byte hashes and invalid Unicode ranges; bridge error classification is tested without exposing provider bodies. | No live hymt/model quality or Telegram playback acceptance is claimed. Semantic fidelity is explicitly unverified; private upstream cache is disposable and currently has no disk quota. Full runtime admission and cache maintenance remain F18/F19. |

The F11 fixture initially assumed every hymt request was streaming; the pinned
implementation uses non-streaming calls for some later segments. The fixture was
corrected to serve both documented client paths. An empty completion with a
protected URL revealed a real adapter validation gap; the bridge now rejects a
result equal to reconstructing all translated segments as empty. Tests preserve
that case. No semantic-completeness guarantee was added.
