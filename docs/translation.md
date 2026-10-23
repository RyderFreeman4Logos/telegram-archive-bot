# Translation adapter (F11)

`Translator` accepts an exact source revision, target language, profile and a
claimed job snapshot/cancellation context. The ADK translation graph calls the
host interface. The primary adapter supervises `archive-hymt-bridge`, a Rust
binary directly linking hymt's core, client, segment, cache and translate crates
at `de670b21b26b3a88a8d711845c9e3d3b9af4c0ba`. It never invokes the hymt CLI.

The bridge has its own workspace and lockfile. A resolver probe with the pinned
host rusqlite 0.32.1/SQLx 0.8.6 and hymt's rusqlite 0.31 failed because two
`libsqlite3-sys` versions declare the same native `sqlite3` link. An older host
SQLx/rusqlite combination can resolve, but changing the established host database
pins would add migration/compatibility work. A process boundary preserves both
dependency graphs without changing upstream or mixing their database handles.

Build both workspaces using the README commands. Set `tools.hymt_bridge` to the
absolute installed bridge path. Translation remains disabled by default. An
enabled configuration requires an explicit deployment revision and model,
profile (`generic`, `hy_mt2_7b`, `hy_mt2_30b_a3b`), backend
(`openai_compatible`, `llama_cpp`, `vllm`), language and context budget. The backend
default is OpenAI compatible; a model profile never injects sampler overrides.
The initial adapter uses hymt's conservative tokenizer fallback and records
`approximate` counting and every segment's planned input tokens. It does not
download a tokenizer implicitly.

Each invocation writes source and mode-0600 job TOML inside a unique mode-0700
workspace. The bridge loads `HotConfig::from_path`, immediately detaches the
snapshot, explicitly preflights the backend, then calls `plan_translation` and
`translate_text_stream`. Network retries and completeness correction retries are
both **zero**. Segment concurrency is one; output reservation is one quarter of
the configured context, capped at 4096. Provider sampler fields are omitted.
The outer scheduler retains its persisted attempts, absolute deadline and one
LLM timeout for the entire translation graph. Starting requires the full budget
to fit. Cancellation/timeout terminates the supervised process group. Later
segments may use hymt's non-streaming path internally; progress never proves
transport completion.

Application caching is deliberately enabled. The bridge always runs a fresh
preflight before consulting hymt's cache and delegates eligibility and replay
proof checks to that pinned implementation. Missing model/runtime/sampler
evidence bypasses eligible application cache reads; matching text alone does not
authorize reuse. Cache files live in private `state/cache/hymt/<config-id>.sqlite`
and remain disposable upstream-owned state. They are separate from the canonical
archive database. The host never opens or migrates them. A configuration change
selects a different cache namespace. The current adapter does not impose a cache
disk quota; cache retention/backup selection belongs to the maintenance feature.

Only a successful final `TranslationOutcome` admits publication. Token and
`AllDone` events are ignored as completion evidence; bounded segment ordinals
become typed stage progress. The host validates the bridge schema, pinned
revision, exact input/output hashes, byte bounds, protected source regions and
retained source/output ranges before publishing CAS blobs and a fenced immutable
derivative. Original revision text never changes. The manifest records language,
profile, job configuration, bridge binary hash, upstream inference fingerprint,
cache verification, token plan, progress and degraded/retained regions.
Attempt records are private. Errors expose fixed categories without raw provider
response bodies. An interrupted attempt can remain historically `running`.

Protocol truncation can produce a marked original-preserving **partial** result
under hymt's warn-only policy. A failed segment or cancellation cannot publish a
successful translation derivative. An empty final result, or a result equal to
reconstructing only the protected regions with all translations empty, is
rejected. The public hymt outcome does not expose each segment's raw completed
response; this adapter cannot prove that an individual nonempty segment retained
all facts, links or meaning. Every manifest sets `semantic_fidelity_verified` to
false. A transport-completed output with an undetected omission is not semantic
acceptance. A separate versioned review stage would be needed for that.

The durable phase output advances to download only after the complete ADK graph
finishes. A death before phase commit can re-execute nodes; verified hymt cache
entries may avoid provider generation, while unverified inputs run again.
Completed phase outputs prevent retranslation after commit. Per-node ADK
checkpoints are currently used by metadata, not this translation graph.
Cross-job coalescing and full runtime admission remain F18/F19 work.

The integrated fixtures exercise the real Rust bridge, mixed-language Markdown,
code/Unicode, multiple segments, verified cache reuse, profile invalidation,
unverified cache bypass, hot-file mutation after the first request, empty output,
truncation with retained originals, a failing later segment, silent timeout,+cancellation and the ADK phase transition. These do not establish live provider
quality or Telegram client playback.

References: [pinned translation exports](https://github.com/RyderFreeman4Logos/hymt/blob/de670b21b26b3a88a8d711845c9e3d3b9af4c0ba/crates/hymt-translate/src/lib.rs),
[configuration and fingerprint rules](https://github.com/RyderFreeman4Logos/hymt/blob/de670b21b26b3a88a8d711845c9e3d3b9af4c0ba/crates/hymt-core/src/config.rs).
