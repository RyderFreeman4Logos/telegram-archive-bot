# Backup and recovery (F17)

SQLite and retained CAS files together are the full archive. A semantic Git
repository is a narrower export. Run these commands with an absolute new
backup/restore destination outside Git worktrees; existing destinations are
never overwritten. The parent directory must already exist.

```sh
telegram-archive-bot --config /absolute/bot.toml backup /private/backups/archive-001
telegram-archive-bot verify-backup /private/backups/archive-001
telegram-archive-bot restore-backup /private/backups/archive-001 /private/restored-state
```

`backup` uses SQLite's online backup API against an established WAL read snapshot.
It holds the CAS publication/GC fence while copying the snapshot's entire
retained blob catalog. Canonical writers using already-published objects can
continue; new blob publication waits for that fence. The backup database uses
DELETE journaling so it is independently readable without a live WAL. Its
manifest includes schema/configuration versions, database SHA-256/length, and a
sorted SHA-256/length inventory for every retained CAS object. File contents,
directories and the final manifest are synced before an atomic no-replace
publication. An interrupted operation leaves no apparently complete destination.
SQLite backup progress has a five-minute deadline.

`verify-backup` requires no bot configuration or credentials. It checks the
manifest policy, exact database digest/length, supported schema, SQLite integrity,
foreign keys, exact catalog/inventory equality and every object digest/length.
Missing/corrupt objects identify their digest. Symlinks and hard links are not
accepted as backup objects. Copy backups independently, not with hard links to
live CAS files. Current backups retain all catalogued objects; disposable
filesystem caches, external model files, credential files and environment values
are excluded. Treat the retained source text/raw payloads themselves as private.

`restore-backup` verifies the supplied backup, copies to an isolated sibling
staging directory, verifies the copied database/objects again, migrates supported
older schemas and writes a recovery report. It preserves canonical records,
configuration references, checkpoints and historical receipts. It increments
active job generations, expires their leases and pauses queued/running/retryable
jobs. Unsent pending/retryable deliveries become paused; in-flight sends become
**unknown**, with their attempt history retained. Previously unknown sends stay
unknown. Pending push attempts become uncertain.
Active daily collections become partial/interrupted with incremented generations;
their cursors and immutable page receipts remain retained. The global hold also
blocks automatic daily admissions and collection claims.

A global recovery hold blocks all job claims, delivery claims and projection,
including newly enqueued work, until explicitly cleared. Verification and
restoration perform no Telegram sends, provider requests, Git pushes or polling.
Point a reviewed configuration at the restored state and arrange credentials
separately before considering activation:

```sh
telegram-archive-bot --config /private/restored.toml recovery-status
telegram-archive-bot --config /private/restored.toml verify
telegram-archive-bot --config /private/restored.toml release-recovery-hold
```

The final command clears only the global hold. It does not restart a service or
unpause old jobs/deliveries. Reconcile them individually through the scoped retry
commands after checking phases, budgets and receipts. Unknown sends require an
explicit duplicate-risk resend; an automatic retry cannot resolve whether the
original Telegram transmission took effect. Restore the separately configured
archive Git repository or project into a new independent destination. Never
restore over a running installation. Optional indexes and filesystem caches can
be rebuilt from authoritative records.

## Git-only semantic recovery

```sh
telegram-archive-bot restore-git /private/cloned-archive /private/semantic-state
```

An optional `--config /absolute/bot.toml` selects the configured Git executable
and resource limits. Without it, safe default tool/limit settings suffice;
provider credentials are not needed. The importer selects the latest committed
`Archive-Manifest` checkpoint, lists its files and hashes captured worktree bytes
against those exact Git blob IDs. Modified bodies, symlinks, unsupported schemas,
unknown paths, missing references and inconsistent content/view digests fail
before publishing a destination. Unrelated worktree files are not imported.

`limits.semantic_files` is a positive file-count bound, defaulting to 100,000.
For a larger archive, explicitly set it to the measured committed file count.
Independently, `limits.input_bytes` bounds the Git tree listing and other Git
stdout, while `limits.work_bytes` bounds the total captured semantic bytes.
Each semantic file is also limited to the smaller of `work_bytes` and 64 MiB.
Restoration holds captured files in memory; size these bounds for available RAM
and storage. No bound is raised automatically, and rejected imports leave no
published destination. These limits do not relax path or committed-object checks.

The current export schema restores source identities, aliases, exact immutable
revisions (including original newline reconstruction), observed relationships,
views, DA01 account/activity discovery evidence and exported text derivatives.
Activity observations retain their original source-observation and revision/view
pins, including unavailable targets and independent activity/publication time
evidence. Older exports without activity records remain supported. Full schema
14 backups also retain every account/activity and operational planner table;
daily plans/checkpoints are not included in semantic Git recovery. Restoring a supported older
database adds the empty tables without changing existing source/view records.
Original observation IDs retain view-edge
provenance. The current revision is selected from the latest available complete
observation, or partial observation when no complete observation exists.

Text derivatives retain their original semantic manifests and available
translation text in explicitly ineligible semantic records. They do not become
verified production cache hits without their original runtime/validation context.
Raw-response hashes and binary artifact manifests are retained as unavailable
provenance, without manufacturing CAS objects or ready audio artifacts. Re-export
can reproduce the original semantic files, including those unavailability
references. The report records the source commit/checkpoint and explicitly lists
missing raw/binary data, recent unexported changes, jobs, receipts, push state,
credentials and disposable caches/indexes. A global recovery hold also protects
this rebuilt installation. Git-only reconstruction is not the optional legacy
format importer F22.

## Repeatable disaster-recovery exercise

1. Capture synthetic A/B sources, a pinned quote view and a retained object.
2. Run `backup`, `verify-backup` and `restore-backup` into fresh paths. Compare
   revisions, aliases, observation/edge/view records and blob inventories; inspect
   recovery status and uncertain sends before clearing any hold.
3. Remove or corrupt a copied backup object; verification must identify the
   missing/corrupt digest and restoration must publish no destination.
4. Export the semantic repository, run `restore-git`, inspect its limitations and
   verify that binary manifests cannot be selected through the ready-artifact API.
5. After an explicit global hold release in the isolated fixture, re-export to a
   new semantic repository and compare semantic files byte for byte.

The automated checks perform this exercise with synthetic data, real Git and a
real three-second FFmpeg fixture. Another test synchronizes a canonical writer
after the SQLite snapshot while a new publisher waits behind the backup fence.
The command-level check invokes the actual debug executable for init, backup,
verification, restore, status and explicit global hold release. These are offline
recovery receipts, not installed-release or live-service activation evidence.
