# Typed metadata workflows (F10)

The application pins ADK core, model and graph libraries to 2.3.0 and enables only
OpenAI-compatible model support and SQLite checkpoints. The graph exposes no
agent tools. Its nodes consume exact immutable revisions and return validated
metadata references. It does not send Telegram messages or manipulate Git.

A versioned metadata result contains a display title, summary, language, tags,
and supporting source-block IDs. Schema, nonempty/domain bounds, duplicate
references and reference membership are validated by the host. Each block stores
its source revision, UTF-8 byte range and content hash. Existence and byte identity
are independently checked; factual fidelity is explicitly **not verified**.
Summaries remain derivatives and never replace source revisions.

The conservative tokenizer fallback budgets one token per UTF-8 byte of the
actual serialized prompt, reserves configured output tokens plus 256 framing
tokens, and splits even one long paragraph at character boundaries. Per-chunk
summaries merge in source order without another model call; title comes from the
first chunk, tags are sorted/deduplicated and mixed output languages become
`mul`. Excessively large merged metadata fails validation rather than silently
truncating its summary. The graph-node limit also bounds the number of chunks.

A single deadline and cancellation cover all graph nodes and the complete model
stream, including a model silent before its first response. Starting an attempt
requires its full configured budget to fit the persisted absolute job deadline.
The OpenAI-compatible client has ADK network retries disabled; no graph retry
policy or protocol-correction loop is installed. The durable scheduler owns
attempts. Refusal, truncation, interrupted streams, malformed JSON, unknown
references and permanent authentication failures remain explicit failures.
Terminal `content_complete` content replaces the accumulated response, including
when it differs from its earlier prefix; it is never appended twice.

`SqliteCheckpointer` shares the canonical database through a small SQLx pool with
WAL, FULL synchronization and foreign keys. Checkpoints contain artifact hashes,
not source bodies. `workflow_runs` records job/configuration/input/thread mapping.
The public `MetadataWorkflow::resume_next` claims only metadata jobs, builds the
model from that claimed job's snapshot, and supervises its lease while invoking
ADK resume. It reads an immutable `view_id` from the fetch phase, commits the
metadata digest and advances to translation. Fetch completeness is propagated;
an absent completeness flag is conservative partial. Full daemon dispatch and
fetch/translation integration remain dependent work.

Graph nodes execute at least once. A unique derivation key includes the ordered
revision IDs, snapshot identity (including configured model deployment and prompt
version), selected tools and workflow version. Validated derivatives are
published with a live lease fence. A rerun before an ADK checkpoint reuses that
committed derivative. A changed configuration creates a different key; mutation
of a claimed snapshot fails its integrity check. Cached blobs are rehashed and
metadata is revalidated before use. Cross-job admission/coalescing is completed
in F18; this foundation guarantees idempotent publication, not one physical
model call across simultaneous jobs.

Requests, complete bounded raw responses, attempt statuses and validated
artifacts are retained privately. A process death or dropped model future may
leave an attempt marked running, which is historical ambiguity, not success.
Transport errors are reduced to fixed typed categories before reaching logs or
checkpoints; response bodies and secrets never become error messages. Request
and response retention does not prove that a remote provider received or billed
a request.

Tests use an injected ADK model, a real local HTTP authentication-error fixture,
a committed-checkpoint interruption, and a child process exiting immediately
after derivative commit but before checkpoint persistence. They demonstrate the
specified restart boundaries and stream behavior; no live-model semantic
acceptance is claimed here.

References: [ADK pinned graph documentation](https://github.com/zavora-ai/adk-rust/blob/v2.3.0/docs/official_docs/agents/graph-agents.md),
[terminal streaming contract](https://github.com/zavora-ai/adk-rust/blob/v2.3.0/adk-core/src/event_stream.rs).
