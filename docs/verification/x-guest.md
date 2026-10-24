# X Article guest fallback evidence

Project-Spec: rust-telegram-archive-bot-v1. Spec-ID: F04, F05, F19, F20.
Tracking: [#50](https://github.com/RyderFreeman4Logos/telegram-archive-bot/issues/50).

The opt-in provider runs only after another provider reports an Article preview
without a hydrated body. It uses a separately configured public web-client bearer;
this is not an official X API credential. Ordinary posts and already hydrated
articles do not activate guest sessions. The existing default provider order and
conservative aggregate partial status remain unchanged.

Focused fixtures cover plain/rich article text, nested results, identity mismatch,
empty bodies, raw-only nontext blocks, selective graph admission, one token refresh,
token reuse and expiry, access denial, missing posts, rate limits and server errors.
They also cover cancellation, the cache-lock deadline, bounded activation bodies,
credential-echo rejection, and authenticated cross-origin/POST redirect rejection.
The overall deadline includes activation, refresh and response reads; their body
bytes share a per-fetch budget. Guest tokens live only in memory. Activation
responses and the first rejected-token response are excluded from storage; the
latter's HTTP status is retained. Final query responses are retained unless they
echo authentication material or fail the bounded JSON scan, in which case content
and raw bytes are discarded with an explicit observation. No private credentials
or corpus URLs are fixtures.

An empty Article block list initially reproduced a false hydrated-body result.
The parser regression then passed after hydration required actual body text.
The focused X, configuration, HTTP and guest tests and all-target Clippy passed.

Independent review found ordinary `/article/` text could misclassify a post and
JSON-escaped authentication text could bypass the literal-byte guard. Both have
observed failing regressions. Article admission now shares parsed X host/path/ID
checks across official and FxEmbed inputs. The actual parser-to-graph fixture
checks ordinary prose, external and lookalike hosts, and genuine Article links.
Authentication checks cover every decoded JSON key and value in the input stream
as well as raw bytes, including the token rejected during the same fetch. A second
review and failing HTTP regression showed that normalizing to `Value` first lost
overwritten duplicate fields. The streaming Serde visitor now examines those
fields before normalization and rejects malformed, trailing or excessive-depth
JSON rather than retaining unchecked raw bytes. The provider/storage fixture
rejects escaped token/bearer content and raw-only fields without creating a
revision or CAS object. Preliminary test compile and temporary-directory
permission errors are retained separately and are not behavioral failure evidence.

On 2026-10-24 at 05:39 UTC, the actual Rust guest provider returned HTTP 200 for
one privately selected public Article. All 132 nonempty text blocks were retained
in the canonical revision (11,127 body bytes); the 51,028-byte raw response was
published to CAS and canonical integrity verification passed. Its raw SHA256 was
`aec6278288528b0f11e66c3bbfe2baac00aeed6f9ab72040c66d338e37475173`.
Seventeen nontext blocks remained raw-only, so the result was **partial**. An
earlier provider-only probe did not persist to CAS and is separate evidence.

This live probe exercises the provider directly, not an installed Telegram
roundtrip. The same article was already hydrated by FxEmbed, so it proves no
incremental corpus coverage. At the 05:26 UTC selection checkpoint, neither the
completed supplement nor the still-running main corpus had an evidenced missing
Article body without another hydrated observation. Final corpus coverage must be
reported separately. The pinned web query can change upstream; denial or missing
body remains an explicit observation, never an invented complete article.

Full frozen gates, independent review, installed checks and runtime activation
have separate receipts; these focused and live probes do not imply those passes.
