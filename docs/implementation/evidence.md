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

| F11 | Build pinned Rust bridge; host `cargo test --locked`; bridge `cargo test --locked --manifest-path integrations/hymt-bridge/Cargo.toml`; formatting and all-target Clippy for both workspaces | Final rerun: 58 host checks and 1 bridge check passed; both all-target Clippy checks passed. Six integrated tests run the actual bridge against local HTTP: mixed Markdown/code/Unicode and multiple segments, verified cache reuse, changed profile invalidation, unverified bypass, detached configuration after file mutation, empty translated payload despite protected URL, retained-original truncation, failing later segment, silent cancellation/timeout, omitted sampler fields and ADK phase transition. A unit check rejects tampered byte hashes and invalid Unicode ranges; bridge error classification is tested without exposing provider bodies. | No live hymt/model quality or Telegram playback acceptance is claimed. Semantic fidelity is explicitly unverified; private upstream cache is disposable and currently has no disk quota. Full runtime admission and cache maintenance remain F18/F19. |

The F11 fixture initially assumed every hymt request was streaming; the pinned
implementation uses non-streaming calls for some later segments. The fixture was
corrected to serve both documented client paths. An empty completion with a
protected URL revealed a real adapter validation gap; the bridge now rejects a
result equal to reconstructing all translated segments as empty. Tests preserve
that case. No semantic-completeness guarantee was added.

The initial F11 commit recorded the 58-check run before its output was inspected;
that run failed the newly added 503 classification assertion. This was a receipt
error, not a successful run. The pinned upstream wraps segment failures in an
additional anyhow context. Classification must inspect the cause chain, not only
the outer message; the regression covers both forms. The final rerun below is the
authoritative receipt.

Final F11 rerun after the classification fix: bridge build, bridge test (1), host
Clippy, and the entire host suite (58) all exited successfully. The normal ADK
multi-node fixtures now use a five-second attempt budget; their previous shared
one-second budget also caused two timeout failures under concurrent SQLite
checkpoint load. The dedicated silent/slow-stream fixture still explicitly uses
one second and still asserts Timeout, while cancellation retains its 30 ms
trigger. Production timeout defaults and implementation are unchanged.

A subsequent compatibility check added one recovery regression: old serialized
configuration snapshots missing a newly defaulted field keep their original
content hash and can resume, while mutation of the normalized claimed snapshot
is rejected. The fence now hashes the original database strings and compares
normalized configuration values separately. After this change, formatting,
all-target host Clippy and all **59 host tests** passed. The bridge was unchanged
from its passing 1-test receipt.

The latest F11 PR run [39989656309](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39989656309)
again failed before code checks: the Azure Ubuntu mirror did not finish package
downloads within 180 seconds. CI now selects the official HTTPS Ubuntu archive
for that mirror and omits optional recommended packages. Timeouts and required
code checks remain in place; no result from this failed run counts as validation.

| F12 | `cargo test --locked`; all-target host Clippy; formatting; real Bubblewrap loopback/capability probe | All 66 host tests passed, including five integrated media checks and two proxy checks. Fixtures use the real application relay and Bubblewrap 0.8.0 with a synthetic yt-dlp: simulation, post-processing filename/extension, failed post-processing, path confinement, missing/failing wrappers without fallback, bounded explicit playlists, private-host rejection with an outside positive control, cancellation after descendant-start observation, and timeout. Proxy checks cover pinned public DNS, mixed private answers, HTTP authority/proxy-header cleanup, private redirect follow-up and forwarding limits. The deployed flag probe showed isolated loopback working and CapEff=0. | Synthetic bytes remain pending-finalization candidates; no playable audio, live yt-dlp site or independently identified inner namespace descendant-reap receipt is claimed. F07 separately tests owned process-group descendant death/reap. Full producer/finalizer/runtime integration remains F14/F19/F20. |

F12 adds a separate bounded metadata stdout allowance while retaining the existing
stderr log limit. Host proxy tasks are cancelled and joined before a result is
selected. The snapshot compatibility regression also removes the newly defaulted
Bubblewrap field, confirming that an older snapshot still resumes without
changing its stored hash.

F12 hosted run [39991618324](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39991618324) failed four media fixtures with ToolExit / missing start marker. Local real-namespace fixtures passed; hosted wrapper stderr was not retained, so AppArmor is a hypothesis until the explicit namespace probe runs. CI now prints a real namespace probe, permits user namespaces nonpersistently on its disposable Ubuntu VM when needed, then requires the same probe and all tests to pass. Production remains fail-closed and changes no host policy. See [Ubuntu 24.04 release notes](https://documentation.ubuntu.com/release-notes/24.04/) for that runner prerequisite.

| F13 | `cargo fmt --all`; `cargo clippy --all-targets --locked -- -D warnings`; `cargo test --locked` | All 72 host tests passed, including six speech fixtures using a supervised synthetic producer. Checks retain full Markdown/code/tables/URLs/references, normalize newlines, distinguish translation input, reject mutated snapshots, invalidate legitimate changed language/voice/rate/seed snapshots, preserve TOML-resolved ordered dialogue and segment-local SRT, bind explicit local flags, isolate simultaneous display names, and reject failed/cancelled/timed-out runs without ready artifacts. | Raw fixture bytes are not playable audio. Segment timing is explicitly unvalidated until F14. Real Edge/local synthesis, audible quality, end-to-end delivery retry and client checks remain F14–F20. No pitch flag was invented; nonzero pitch fails configuration validation. |
Hosted [39992796291](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39992796291) confirmed `bwrap: loopback: Failed RTM_NEWADDR: Operation not permitted`; enabling user namespaces on that disposable runner made the namespace probe and all five media fixtures pass. This run still failed the translation snapshot fixture's five-second first-POST wait, so it is not a passing full gate. That fixture now holds the first response until the host has replaced the TOML, using an explicit notification instead of a 50 ms race. Its ordinary startup/operation budgets are 25/30 seconds with early child-exit reporting; the dedicated one-second timeout and cancellation fixtures are unchanged. All six translation fixtures then passed locally. An initial temporary-worktree attempt could not run because its target/bridge fixture paths had not been provisioned; no behavioral result was inferred from that attempt.

| F14 | Formatting; all-target Clippy; `cargo test --locked` | All 80 host tests passed, including seven integrated audio fixtures and one metadata/cue unit check. Real FFmpeg/ffprobe 5.1.9 passed normal MP3/M4A/Opus, missing-duration remux/reinspection, no-Xing MP3, rejected truncation/low bitrate/oversize/bad batch, cancellation after observed decoder startup, one-second deadline, hashes, profile conversion, and measured subtitle offsets. The 3+17 s concatenation fixture asserts that successful stream-copy remux still fails strict decode; explicit transcode then passes around 20 s with raw diagnostics retained. | Missing probe duration is injected by a wrapper around the real probe. No original-content completeness, audible quality, Telegram seeking/speed UI or real producer integration is claimed. Oversized files fail clearly rather than silently reducing quality. |

An earlier full F14 candidate run passed audio and the preceding suites, then failed two X fixtures with ENOENT in their system-temporary storage. A focused strace rerun passed and did not reproduce the failure; it does not establish a new exact cause. Those fixtures now use repository-owned target directories, consistent with the earlier independently diagnosed F09 temporary-directory isolation issue. The complete 80-test gate above passed after that fixture-only change; production storage error handling was not weakened.

F14 hosted [39994858311](https://github.com/RyderFreeman4Logos/telegram-archive-bot/actions/runs/39994858311) failed a fixture assumption: that host's remuxed concatenation decoded cleanly, unlike local FFmpeg 5.1.9. The validator itself was unchanged. The corrected fixture requires independent full decode, duration consistency and the expected 20-second timeline when remux is usable, and requires rejection/transcode when it is not. All seven real-audio fixtures and all-target Clippy passed locally after this correction. A new hosted full gate is still required.
