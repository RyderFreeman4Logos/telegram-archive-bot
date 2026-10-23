# Semantic Git archive (F16)

`telegram-archive-bot --config /absolute/bot.toml project --limit 100`
processes at most 100 persisted knowledge-event batches. Configure an independent
absolute `[archive].directory`; a new destination gets a fresh Git repository.
Existing repositories must be non-bare, on the configured branch. Push defaults
to false and additionally requires explicit `remote` and `branch`.

The destination must not overlap the application checkout or canonical state.
Application Git metadata/remotes are rejected as archive destinations. A private
binding in the archive's Git metadata prevents two canonical databases from
mixing their event streams. Full backup restores preserve that database identity.
Git clones carry semantic files, not this local binding.

`archive-v1/` contains versioned, sorted JSON and LF UTF-8 text. Sources have
stable identity records, and each revision has one `body.md` plus metadata.
Views refer to exact revision IDs, with explicit reasons for missing nodes;
all referenced bodies are included. Observation records retain original fetch
times, evidence and directed relation edges. Aliases retain their original URL
and normalization method. An original-newline map allows an importer to recover
the precise canonical body even when the export uses LF.

Textual metadata, translations and spoken-text manifests are exported.
Candidate/diagnostic derivatives export a descriptor and digest only; raw
provider responses, request logs, runtime configuration, credentials, queues,
receipts and binary CAS objects are not copied. Validated artifact manifests
retain their media provenance, but a digest-only export is not a binary backup.
The destination can contain private source text and must use the operator's
intended access controls, independently of the public application repository.

Capture revision/alias/observation events commit as one knowledge event. Other
immutable view, derivative and artifact events each define a logical event.
The projector takes a consistent SQLite snapshot under the CAS publication
fence, then releases database transactions before running Git. Both canonical
store and archive Git metadata have serial projector locks.

An intent records exact rendered bytes, base ref, previous managed index entries
and event/hash marker before writes. Files use synced atomic replacement.
Git plumbing builds a temporary index from HEAD with only the explicit event
paths, then commits and advances the checked-out ref with compare-and-swap.
A separate copy of the user's index receives just these managed entries under
Git's normal index lock; unrelated staged and worktree changes stay intact.
Managed staged/worktree edits, deletions, symlinks and hard links fail rather
than being incorporated or overwritten. Automatic commits use the explicit
service identity `Telegram Archive <telegram-archive@localhost>` and bypass
repository hooks/filters through plumbing. Operator Git configuration is not
modified. No reset, force push or unrelated staging is performed.

After process death, the persisted commit ID, event trailer and expected Git
blob hashes reconcile a committed event without another semantic commit.
An empty diff advances the checkpoint successfully. An owned stale index lock
is removed only when its device, inode and complete expected bytes match the
intent. An unknown lock or torn index write requires operator inspection; it is
never blindly deleted. Avoid editing managed paths while projection is running.

Local cursor state and each configured remote target's push state are separate.
Before retrying a push, and after any result including timeout/nonzero exit, the
projector queries that exact push URL/ref. It can fetch objects without updating
worktree refs to check ancestry. A remotely reachable expected commit confirms
success even if the push command lost its response. Otherwise the push remains
rejected/uncertain with the local archive intact. Interactive Git credentials
are disabled and embedded URL secrets are rejected; arrange transport access
outside source/config exports. Git failures do not undo canonical captures.

Tests use synthetic content in independent real Git repositories, child exits
after file writes and after commit, concurrent requests, a rejecting bare remote
and a wrapper that successfully pushes then exits nonzero. They do not publish
user archive content or claim a real remote credential check.
