# Canonical local storage

`init`, `verify`, and `gc` use the configured absolute `state_dir`. Create its
parent first. Existing managed directories must belong to the current user and
have mode `0700`; the service creates its own directories with that mode. Keep
this state on a local filesystem. SQLite uses WAL, FULL synchronization, a five
second busy timeout, foreign keys on every connection, and versioned transactional
STRICT migrations. A newer schema is rejected. No external work runs inside a
SQLite write transaction.

Clones of a store reuse up to eight idle connections. Active operations retain
exclusive ownership of their connection; this idle limit does not cap worker
concurrency. Dropping an unfinished transaction closes that connection instead
of returning it to another worker. Keeping idle connections open avoids repeated
WAL checkpoints between short operations. FULL synchronization and the existing
backup, restore and publication fences remain in force.

The [connection reuse measurement](verification/storage-performance.md) records
the public replay comparison and its limits.

`archive.sqlite` owns canonical text and operational records. `blobs/<sha256>`
owns immutable retained bytes. `staging/` holds incomplete publications. A blob
catalogue entry proves byte durability only; a media artifact additionally needs
its profile validation report before it can be delivered.

Publication writes a uniquely named file on the same filesystem, enforces a byte
limit, hashes and fsyncs it, hard-links it to its digest without overwriting,
fsyncs the object directory, and then commits the catalogue reference. Existing
objects must match both length and digest. Verification reports missing/corrupt
objects as errors. A file lock fences publication through catalogue commit; GC
and full backups acquire the same lock. The kernel releases it on process death,
and an old process cannot resume with an expired generation. This is the local
single-host equivalent of publication leases; no time-based lease is stolen.

GC removes only uncatalogued objects and abandoned staging files older than
`limits.gc_grace_seconds`. All catalogued blobs remain retained. It rejects
unexpected symlinks and leaves referenced objects intact. Reclaiming deliberately
retired catalogue entries is outside this release's retention policy.

Tests cover eight concurrent identical publishers, foreign-key enforcement,
reopening the supported schema, size limits, symlinks, corruption, interruption
at each publication checkpoint, actual child-process exit without destructors,
and GC blocked by an active publisher. Process exit tests do not simulate a
machine power failure; filesystem durability relies on the fsync contract.

## Source revisions and pinned views

X identities use the numeric status ID across supported x.com/twitter.com hosts
and author handles. Generic URLs retain query parameters and fragments. Submitted
aliases remain evidence and never merge unrelated sources by title or similarity.
Normalized body and typed meaningful metadata form an immutable revision hash;
counters, extraction versions and observed times belong to separate observations.
Complete observations select the current revision. A preview or failed/deleted
fetch cannot replace an existing complete capture. A later complete edit creates
a new revision while earlier views retain their exact revision IDs.

Schema 2 moves completeness entirely into observations: a partial and a complete
fetch with identical normalized content can share one revision. Migration from
schema 1 preserves its existing records. Typed quote/reply/retweet/link/edit edges
retain their establishing observation and support unresolved source stubs.

Views are immutable content-addressed manifests. Breadth-first expansion uses
stable edge ordering, a visited set, explicit node/depth limits, and recorded
omissions. Missing nodes remain explicit; hydration creates a new view. The
manifest describes the revisions actually archived, without claiming which
revision a historical author saw. Revision/observation/view changes also create
projection events in the same short transaction.

## Account activities (DA01 / #68)

Schema 13 adds accounts, account observations, discovery runs, activities and
activity observations. It adds no source body store and changes no existing
source, revision or view rows. `Store::record_activity` captures a source using
the existing content hash, then pins its current archived revision and context
view in the same SQLite writer transaction. Account aliases, counters, retrieval
times and discovery paths remain outside that content hash. An activity records
an `original`, `quote`, `reply` or `repost` relation to its source. A quote/reply
source's context continues to use the existing typed source edges. Multiple
repost actors reference one original source rather than copying its body.

`AccountIdentity::ProviderId` uses the provider and user ID. Observed handles and
display names are immutable history associated with that identity; a rename
does not create another account. When the provider does not establish a user ID,
the caller supplies an explicit `Unresolved` key. A matching handle is never
proof for merging uncertain accounts with each other or with a known user ID.

An activity's provider event ID is its stable identity when supplied. The actor,
kind and source assertions cannot change under that identity. Without a provider
event ID, the fallback is scoped to the actor, source, discovery run and its
caller-supplied occurrence key. Distinct occurrences, including repeated reposts by the same
actor of the same source, must have distinct keys. The fallback deliberately
claims no cross-run identity. An adapter must not manufacture a provider event
ID from the original post ID or from actor/target equality.

Discovery runs have stable provider/key identities and immutable provenance.
Each retrieval supplies an `observation_key`, stable for identical retries. A
new fetch, discovery path, rename or hydration supplies a new observation key;
reuse with a different payload is refused rather than replacing prior evidence.
Concurrent identical workers converge to the same activity observation and
source revision. Separate page observations may retain distinct discovery
evidence while sharing the same provider activity ID and source revision.

Activity time and original publication time are independent `TimeEvidence`
values. An instant or inclusive interval retains UTC seconds/nanoseconds,
reported precision and provider provenance (which can retain the original time
string/timezone). Unknown time remains explicit. Neither is substituted for the
other. Local retrieval time is separately recorded as UTC epoch seconds. This
lets a recent repost refer to an old original without shifting either timestamp.

Each activity observation retains its source observation ID, fetch status, view
ID, exact archived root revision or explicit missing reason. A missing target
can later be hydrated, and an edit can create another revision, without changing
earlier activity observations or views. A failed/deleted fetch can still pin an
earlier complete archived revision; its fetch status continues to record that
the fresh fetch failed. No claim is made about the historical revision seen by
the actor. `activity`, `activity_observation(s)`, `account_activities` and
`account_history` expose persisted records without starting a provider or job.

Full backups preserve these tables. Semantic Git exports include immutable
`archive-v1/activity-observations/<id>.json` manifests with account/run evidence
and references to the existing source observations and pinned views. Git-only
restore validates those identities and references after importing the canonical
sources/views. Recovery holds still block projection and other external side
effects. Synthetic tests cover ordering, retry/concurrency convergence, aliases,
unknown identities/times, edits, hydration, schema 12 upgrade, full backup and
Git semantic restore/re-export. Discovery, period selection and briefing jobs
are separate work; this persistence API does not schedule or contact providers.
