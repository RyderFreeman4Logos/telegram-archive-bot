# Canonical local storage

`init`, `verify`, and `gc` use the configured absolute `state_dir`. Create its
parent first. Existing managed directories must belong to the current user and
have mode `0700`; the service creates its own directories with that mode. Keep
this state on a local filesystem. SQLite uses WAL, FULL synchronization, a five
second busy timeout, foreign keys on every connection, and versioned transactional
STRICT migrations. A newer schema is rejected. No external work runs inside a
SQLite write transaction.

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
