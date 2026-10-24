//! Full retained-state backup and deliberately narrower semantic reconstruction.
use crate::{
    config::Config,
    digest,
    process::Tool,
    storage::{Blob, SCHEMA_VERSION, Store, now, private_dir, sync_dir},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{CString, OsString},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::{
        ffi::OsStrExt,
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::Path,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupManifest {
    pub schema_version: u32,
    pub database_schema: u32,
    pub configuration_schema: u32,
    pub created_at: i64,
    pub database: Blob,
    pub blobs: Vec<Blob>,
    pub credentials_included: bool,
    pub caches_included: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub mode: String,
    pub sources: u64,
    pub revisions: u64,
    pub views: u64,
    pub retained_blobs: usize,
    pub external_side_effects_paused: bool,
    pub checkpoint: Option<String>,
    pub limitations: Vec<String>,
}
fn object(path: &Path) -> Result<Blob> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("missing backup object {}", path.display()))?;
    let meta = file.metadata()?;
    ensure!(
        meta.is_file() && meta.nlink() == 1,
        "backup object is not an independent regular file: {}",
        path.display()
    );
    let mut hash = Sha256::new();
    let size = std::io::copy(&mut file, &mut hash)?;
    ensure!(
        size == meta.len(),
        "backup object changed during read: {}",
        path.display()
    );
    Ok(Blob {
        digest: format!("{:x}", hash.finalize()),
        size,
    })
}
fn json_file(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}
fn readonly(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    conn.busy_timeout(Duration::from_secs(5))?;
    conn.pragma_update(None, "foreign_keys", true)?;
    Ok(conn)
}
fn check_database(conn: &Connection) -> Result<u32> {
    let schema: u32 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
    ensure!(
        schema > 0 && schema <= SCHEMA_VERSION,
        "unsupported backup database schema {schema}"
    );
    ensure!(
        conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))? == "ok",
        "backup SQLite integrity check failed"
    );
    ensure!(
        !conn.prepare("PRAGMA foreign_key_check")?.exists([])?,
        "backup foreign key check failed"
    );
    Ok(schema)
}
fn inventory(conn: &Connection) -> Result<Vec<Blob>> {
    Ok(conn
        .prepare("SELECT digest,size FROM blobs ORDER BY digest")?
        .query_map([], |r| {
            Ok(Blob {
                digest: r.get(0)?,
                size: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}
fn verify_objects(root: &Path, blobs: &[Blob]) -> Result<()> {
    private_dir(&root.join("blobs"))?;
    for blob in blobs {
        ensure!(
            blob.digest.len() == 64
                && blob
                    .digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid inventory digest"
        );
        ensure!(
            object(&root.join("blobs").join(&blob.digest))? == *blob,
            "missing/corrupt retained object {} (expected {} bytes)",
            blob.digest,
            blob.size
        );
    }
    Ok(())
}
fn stage(destination: &Path, source: &Path) -> Result<tempfile::TempDir> {
    ensure!(
        destination.is_absolute()
            && !destination.exists()
            && destination.symlink_metadata().is_err(),
        "recovery destination must be a new absolute path"
    );
    let parent = destination
        .parent()
        .context("destination has no parent")?
        .canonicalize()?;
    ensure!(
        !parent.ancestors().any(|p| p.join(".git").exists()),
        "private recovery destinations must be outside Git worktrees"
    );
    let resolved = parent.join(destination.file_name().context("destination has no name")?);
    let source = source.canonicalize()?;
    ensure!(
        !resolved.starts_with(&source) && !source.starts_with(&resolved),
        "recovery destination overlaps source"
    );
    if let Ok(code) = Path::new(env!("CARGO_MANIFEST_DIR")).canonicalize() {
        ensure!(
            !resolved.starts_with(&code) && !code.starts_with(&resolved),
            "private recovery data cannot be written into public code checkout"
        );
    }
    Ok(tempfile::Builder::new()
        .prefix(".archive-recovery-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(parent)?)
}
fn publish(stage: tempfile::TempDir, destination: &Path) -> Result<()> {
    let from = CString::new(stage.path().as_os_str().as_bytes())?;
    let to = CString::new(destination.as_os_str().as_bytes())?;
    // Linux no-replace rename makes a concurrent destination creator a safe failure.
    ensure!(
        unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        } == 0,
        "cannot publish isolated recovery destination: {}",
        std::io::Error::last_os_error()
    );
    sync_dir(destination.parent().unwrap())?;
    let _ = stage.keep();
    Ok(())
}
fn report(
    store: &Store,
    mode: &str,
    checkpoint: Option<String>,
    limitations: Vec<String>,
) -> Result<RecoveryReport> {
    let (sources, revisions, _) = store.source_counts()?;
    let views = store
        .connection()?
        .query_row("SELECT count(*) FROM views", [], |r| r.get(0))?;
    Ok(RecoveryReport {
        mode: mode.into(),
        sources,
        revisions,
        views,
        retained_blobs: store.integrity()?,
        external_side_effects_paused: store.recovery_paused()?,
        checkpoint,
        limitations,
    })
}
impl Store {
    pub fn recovery_paused(&self) -> Result<bool> {
        Ok(self.connection()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM operational_holds)",
            [],
            |r| r.get(0),
        )?)
    }
    /// Clears the global hold only. Old jobs/outbox rows remain individually paused or unknown.
    pub fn release_recovery_hold(&self) -> Result<()> {
        self.connection()?
            .execute("DELETE FROM operational_holds WHERE name='restore'", [])?;
        Ok(())
    }
    pub fn backup(&self, destination: &Path) -> Result<BackupManifest> {
        self.backup_inner(destination, || {})
    }
    fn backup_inner(
        &self,
        destination: &Path,
        after_snapshot: impl FnOnce(),
    ) -> Result<BackupManifest> {
        let stage = stage(destination, self.root())?;
        private_dir(&stage.path().join("blobs"))?;
        let _pin = self.publication_lock()?;
        let mut source = self.connection()?;
        let tx = source.transaction()?;
        // Establish a WAL read snapshot; ordinary canonical writers can continue.
        tx.query_row("SELECT count(*) FROM blobs", [], |r| r.get::<_, u64>(0))?;
        let db = stage.path().join("archive.sqlite");
        let mut to = Connection::open(&db)?;
        {
            let backup = rusqlite::backup::Backup::new(&tx, &mut to)?;
            let start = Instant::now();
            loop {
                match backup.step(256)? {
                    rusqlite::backup::StepResult::Done => break,
                    _ => {
                        ensure!(
                            start.elapsed() < Duration::from_secs(300),
                            "SQLite backup deadline exceeded"
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            }
        }
        tx.commit()?;
        to.pragma_update(None, "journal_mode", "DELETE")?;
        let schema = check_database(&to)?;
        let blobs = inventory(&to)?;
        drop(to);
        after_snapshot();
        fs::set_permissions(&db, fs::Permissions::from_mode(0o600))?;
        File::open(&db)?.sync_all()?;
        for blob in &blobs {
            let from = self.blob_path(&blob.digest)?;
            ensure!(
                object(&from)? == *blob,
                "corrupt retained object {}",
                blob.digest
            );
            let to = stage.path().join("blobs").join(&blob.digest);
            fs::copy(from, &to)?;
            fs::set_permissions(&to, fs::Permissions::from_mode(0o600))?;
            File::open(to)?.sync_all()?;
        }
        verify_objects(stage.path(), &blobs)?;
        let manifest = BackupManifest {
            schema_version: 1,
            database_schema: schema,
            configuration_schema: 1,
            created_at: now(),
            database: object(&db)?,
            blobs,
            credentials_included: false,
            caches_included: false,
        };
        json_file(&stage.path().join("manifest.json"), &manifest)?;
        sync_dir(&stage.path().join("blobs"))?;
        sync_dir(stage.path())?;
        publish(stage, destination)?;
        Ok(manifest)
    }
    pub fn verify_backup(root: &Path) -> Result<BackupManifest> {
        ensure!(root.is_absolute(), "backup path must be absolute");
        private_dir(root)?;
        let path = root.join("manifest.json");
        ensure!(
            object(&path)?.size <= 64 * 1024 * 1024,
            "backup manifest too large"
        );
        let manifest: BackupManifest = serde_json::from_slice(&fs::read(path)?)?;
        ensure!(
            manifest.schema_version == 1
                && manifest.configuration_schema == 1
                && !manifest.credentials_included
                && !manifest.caches_included,
            "unsupported backup manifest policy"
        );
        let db = root.join("archive.sqlite");
        ensure!(
            object(&db)? == manifest.database,
            "backup database digest/length mismatch"
        );
        let conn = readonly(&db)?;
        ensure!(
            check_database(&conn)? == manifest.database_schema,
            "backup schema manifest mismatch"
        );
        ensure!(
            inventory(&conn)? == manifest.blobs,
            "backup inventory does not match database catalog"
        );
        verify_objects(root, &manifest.blobs)?;
        Ok(manifest)
    }
    pub fn restore_backup(backup: &Path, destination: &Path) -> Result<RecoveryReport> {
        let manifest = Self::verify_backup(backup)?;
        let stage = stage(destination, backup)?;
        fs::copy(
            backup.join("archive.sqlite"),
            stage.path().join("archive.sqlite"),
        )?;
        fs::set_permissions(
            stage.path().join("archive.sqlite"),
            fs::Permissions::from_mode(0o600),
        )?;
        ensure!(
            object(&stage.path().join("archive.sqlite"))? == manifest.database,
            "backup database changed while restoring"
        );
        let store = Self::open(stage.path())?;
        for blob in &manifest.blobs {
            let to = stage.path().join("blobs").join(&blob.digest);
            fs::copy(backup.join("blobs").join(&blob.digest), &to)?;
            fs::set_permissions(&to, fs::Permissions::from_mode(0o600))?;
            File::open(to)?.sync_all()?;
        }
        store.pause_recovered()?;
        let receipt=report(&store,"full",None,vec!["Credentials and disposable filesystem caches must be configured separately.".into(),"Old jobs and deliveries require explicit individual reconciliation; uncertain sends retain ambiguity.".into()])?;
        json_file(&stage.path().join("restore-report.json"), &receipt)?;
        store
            .connection()?
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        File::open(stage.path().join("archive.sqlite"))?.sync_all()?;
        sync_dir(&stage.path().join("blobs"))?;
        sync_dir(stage.path())?;
        drop(store);
        publish(stage, destination)?;
        Ok(receipt)
    }
    fn pause_recovered(&self) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute("INSERT INTO operational_holds VALUES('restore','External side effects require explicit recovery reconciliation') ON CONFLICT DO NOTHING",[])?;
        tx.execute("UPDATE jobs SET state='paused',lease_until=NULL,generation=generation+1,error='restored_paused' WHERE state IN ('queued','running','retryable','paused')",[])?;
        tx.execute("UPDATE delivery_attempts SET state='unknown',finished_at=?1,error='restored_uncertain' WHERE state='sending'",[now()])?;
        tx.execute("UPDATE outbox SET state='unknown',lease_until=NULL,error='restored_uncertain' WHERE state='sending'",[])?;
        tx.execute("UPDATE outbox SET state='paused',lease_until=NULL,error='restored_paused' WHERE state IN ('pending','retryable')",[])?;
        tx.execute(
            "UPDATE projection_pushes SET state='uncertain' WHERE state='pending'",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }
}

async fn read_git(
    config: &Config,
    archive: &Path,
    work: &Path,
    args: &[&str],
    cancel: &CancellationToken,
) -> Result<String> {
    let mut argv = vec![
        "-c".into(),
        "core.hooksPath=/dev/null".into(),
        "-c".into(),
        "core.fsmonitor=false".into(),
        "-C".into(),
        archive.as_os_str().to_owned(),
    ];
    argv.extend(args.iter().map(OsString::from));
    let tool = Tool {
        executable: config.tools.git.clone(),
        args: argv,
        environment: BTreeMap::from([
            ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
            ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
            ("GIT_OPTIONAL_LOCKS".into(), "0".into()),
        ]),
        directory: work.to_owned(),
        timeout: Duration::from_secs(config.limits.media_seconds),
        log_bytes: config.limits.log_bytes,
        stdout_bytes: Some(config.limits.input_bytes as usize),
        work_bytes: config.limits.work_bytes,
        secrets: vec![],
    };
    Ok(tool.run(cancel).await?.stdout.trim_end_matches('\n').into())
}
fn export_file(root: &Path, path: &str, limit: u64) -> Result<Vec<u8>> {
    ensure!(
        path.starts_with("archive-v1/")
            && path
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/_.-".contains(&b)),
        "unsafe semantic export path"
    );
    let mut current = root.to_owned();
    for part in Path::new(path).components() {
        ensure!(
            matches!(part, std::path::Component::Normal(_)),
            "unsafe export component"
        );
        current.push(part);
        ensure!(
            !current.symlink_metadata()?.file_type().is_symlink(),
            "symlink in semantic export"
        );
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&current)?;
    let meta = file.metadata()?;
    ensure!(
        meta.is_file() && meta.nlink() == 1 && meta.len() <= limit,
        "invalid/oversized semantic file"
    );
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= limit,
        "semantic file exceeds byte bound"
    );
    Ok(bytes)
}
fn value_string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .with_context(|| format!("semantic record lacks {key}"))
}
fn version(value: &Value) -> Result<()> {
    ensure!(
        value["schema_version"] == 1,
        "unsupported semantic record schema"
    );
    Ok(())
}
fn canonical_body(normalized: &[u8], map: &Value) -> Result<String> {
    let mappings: Vec<(usize, bool)> = serde_json::from_value(map.clone())?;
    let mut restored = Vec::new();
    let mut previous = 0;
    for (offset, paired) in mappings {
        ensure!(
            offset >= previous && normalized.get(offset) == Some(&b'\n'),
            "invalid original newline map"
        );
        restored.extend_from_slice(&normalized[previous..offset]);
        restored.push(b'\r');
        if paired {
            restored.push(b'\n');
        }
        previous = offset + 1;
    }
    restored.extend_from_slice(&normalized[previous..]);
    Ok(String::from_utf8(restored)?)
}
impl Store {
    /// Import only a committed current-schema semantic export. No transport or worker is started.
    pub async fn restore_git(
        archive: &Path,
        destination: &Path,
        config: &Config,
        cancel: &CancellationToken,
    ) -> Result<RecoveryReport> {
        ensure!(
            archive.is_absolute(),
            "semantic archive path must be absolute"
        );
        let archive = archive.canonicalize()?;
        let stage = stage(destination, &archive)?;
        let store = Self::open(stage.path())?;
        store.pause_recovered()?;
        let work = store.workspace("semantic-import")?;
        let top = read_git(
            config,
            &archive,
            work.path(),
            &["rev-parse", "--show-toplevel"],
            cancel,
        )
        .await?;
        ensure!(
            Path::new(&top).canonicalize()? == archive,
            "semantic input must be a Git worktree root"
        );
        let checkpoint = read_git(
            config,
            &archive,
            work.path(),
            &["log", "-1", "--format=%H%n%B", "--grep=Archive-Manifest:"],
            cancel,
        )
        .await?;
        let (commit, _) = checkpoint
            .split_once('\n')
            .context("no committed semantic checkpoint found")?;
        ensure!(
            matches!(commit.len(), 40 | 64) && commit.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid semantic checkpoint commit"
        );
        let listing = read_git(
            config,
            &archive,
            work.path(),
            &["ls-tree", "-r", commit, "--", "archive-v1/"],
            cancel,
        )
        .await?;
        let mut files = BTreeMap::new();
        let mut expected = BTreeMap::new();
        let mut total = 0u64;
        for line in listing.lines() {
            let (entry, path) = line
                .split_once('\t')
                .context("invalid semantic tree entry")?;
            let parts: Vec<_> = entry.split_whitespace().collect();
            ensure!(
                parts.len() == 3 && parts[0] == "100644" && parts[1] == "blob",
                "semantic tree contains non-file object"
            );
            let bytes = export_file(
                &archive,
                path,
                config.limits.work_bytes.min(64 * 1024 * 1024),
            )?;
            total = total
                .checked_add(bytes.len() as u64)
                .context("semantic size overflow")?;
            ensure!(
                total <= config.limits.work_bytes && files.len() < config.limits.semantic_files,
                "semantic import exceeds configured bound"
            );
            expected.insert(path.to_owned(), parts[2].to_owned());
            files.insert(path.to_owned(), bytes);
        }
        // Hash the captured bytes, rather than a second read of a mutable worktree.
        let names: Vec<_> = files.keys().cloned().collect();
        for batch in names.chunks(100) {
            let mut paths = Vec::new();
            for (i, name) in batch.iter().enumerate() {
                let path = work.path().join(format!("blob-{i}"));
                fs::write(&path, &files[name])?;
                paths.push(path);
            }
            let mut args = vec!["hash-object", "--no-filters"];
            for path in &paths {
                args.push(path.to_str().context("invalid import workspace")?);
            }
            let hashes = read_git(config, &archive, work.path(), &args, cancel).await?;
            let hashes: Vec<_> = hashes.lines().collect();
            ensure!(
                hashes.len() == batch.len(),
                "semantic object hash count mismatch"
            );
            for ((name, hash), path) in batch.iter().zip(hashes).zip(paths) {
                ensure!(
                    expected[name] == hash,
                    "semantic worktree differs from committed checkpoint: {name}"
                );
                fs::remove_file(path)?;
            }
        }
        let schema: Value = serde_json::from_slice(
            files
                .get("archive-v1/schema.json")
                .context("semantic schema file missing")?,
        )?;
        version(&schema)?;
        ensure!(
            schema["kind"] == "semantic-archive"
                && schema["binary_blobs_included"] == false
                && schema["operational_state_included"] == false,
            "unexpected semantic export policy"
        );
        store.import_semantics(&files)?;
        let receipt=report(&store,"git-only",Some(checkpoint),vec!["Restored only committed semantic content at the exported checkpoint; newer unexported data is absent.".into(),"Original/raw and binary audio bytes, job history, delivery receipts and external push state are unavailable.".into(),"Imported text derivatives are retained as semantic records and are never eligible as verified production cache hits.".into(),"Credentials and disposable indexes/caches must be configured or rebuilt separately.".into()])?;
        json_file(&stage.path().join("restore-report.json"), &receipt)?;
        drop(work);
        store
            .connection()?
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        File::open(stage.path().join("archive.sqlite"))?.sync_all()?;
        sync_dir(&stage.path().join("blobs"))?;
        sync_dir(stage.path())?;
        drop(store);
        publish(stage, destination)?;
        Ok(receipt)
    }
    pub fn semantic_derivative(&self, key: &str) -> Result<Option<Value>> {
        self.connection()?
            .query_row(
                "SELECT manifest FROM semantic_derivatives WHERE derivation_key=?1",
                [key],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .map(|s| serde_json::from_str(&s).map_err(Into::into))
            .transpose()
    }
    fn import_semantics(&self, files: &BTreeMap<String, Vec<u8>>) -> Result<()> {
        let mut records = Vec::new();
        let mut text_blobs = BTreeMap::new();
        for (path, bytes) in files {
            if path.ends_with(".json") && path != "archive-v1/schema.json" {
                let value: Value = serde_json::from_slice(bytes)?;
                version(&value)?;
                records.push((path.clone(), value));
            } else if let Some(rest) = path.strip_prefix("archive-v1/derivatives/")
                && let Some(key) = rest.strip_suffix("/translation.md")
            {
                self.blob_path(key)?;
                String::from_utf8(bytes.clone())?;
                text_blobs.insert(key.to_owned(), self.publish_bytes(bytes)?.digest);
            }
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut consumed = BTreeSet::from(["archive-v1/schema.json".to_owned()]);
        for (path, value) in &records {
            if path.starts_with("archive-v1/sources/") {
                let source = crate::sources::Identity {
                    provider: value_string(value, "provider")?.into(),
                    external_id: value_string(value, "external_id")?.into(),
                };
                let id = source.id();
                ensure!(
                    value_string(value, "id")? == id
                        && *path == format!("archive-v1/sources/{id}.json"),
                    "source identity/path mismatch"
                );
                crate::sources::ensure_source(&tx, &source)?;
                consumed.insert(path.clone());
            }
        }
        for (path, value) in &records {
            if path.starts_with("archive-v1/revisions/") {
                let id = value_string(value, "id")?;
                self.blob_path(id)?;
                ensure!(
                    *path == format!("archive-v1/revisions/{id}/metadata.json")
                        && value["body_file"] == "body.md",
                    "revision path mismatch"
                );
                let body_path = format!("archive-v1/revisions/{id}/body.md");
                let body = files
                    .get(&body_path)
                    .context("pinned revision body missing")?;
                ensure!(
                    digest(body) == value_string(value, "body_file_sha256")?
                        && !body.contains(&b'\r'),
                    "export body digest/newline mismatch"
                );
                let content = crate::sources::Content {
                    body: canonical_body(body, &value["original_newline_map"])?,
                    metadata: serde_json::from_value(value["metadata"].clone())?,
                };
                let source = value_string(value, "source_id")?;
                let hash = digest(serde_json::to_vec(&content)?);
                ensure!(
                    hash == value_string(value, "content_digest")?
                        && digest(format!("{source}:{hash}")) == id,
                    "imported revision content digest mismatch"
                );
                tx.execute(
                    "INSERT INTO source_revisions VALUES(?1,?2,?3,?4,?5)",
                    params![
                        id,
                        source,
                        hash,
                        content.body,
                        serde_json::to_string(&content.metadata)?
                    ],
                )?;
                tx.execute(
                    "INSERT INTO projection_events(kind,entity_id) VALUES('revision',?1)",
                    [id],
                )?;
                consumed.extend([path.clone(), body_path]);
            }
        }
        for (path, value) in &records {
            if path.starts_with("archive-v1/aliases/") {
                let source = value_string(value, "source_id")?;
                let url = value_string(value, "url")?;
                let method = value_string(value, "method")?;
                let valid = match method {
                    "provider_identity_v1" => {
                        crate::sources::Identity::from_url(url)?.id() == source
                    }
                    "yt_dlp_native_v1" => {
                        crate::network::source_url(url)?;
                        let (provider, external): (String, String) = tx.query_row(
                            "SELECT provider,external_id FROM sources WHERE id=?1",
                            [source],
                            |r| Ok((r.get(0)?, r.get(1)?)),
                        )?;
                        records.iter().any(|(path, observation)| {
                            path.starts_with("archive-v1/observations/")
                                && observation["source_id"] == source
                                && observation["extractor"] == "yt-dlp-probe-v1"
                                && observation["evidence"]["submitted_url"] == url
                                && observation["evidence"]["native"]["id"] == external
                                && observation["evidence"]["native"]["extractor"]
                                    .as_str()
                                    .is_some_and(|name| {
                                        provider == format!("media:{}", name.to_ascii_lowercase())
                                    })
                        })
                    }
                    _ => false,
                };
                ensure!(valid, "unsupported alias identity/method");
                let encoded = serde_json::to_string(&(source, url, method))?;
                ensure!(
                    *path == format!("archive-v1/aliases/{}.json", digest(encoded)),
                    "alias path digest mismatch"
                );
                tx.execute(
                    "INSERT INTO source_aliases VALUES(?1,?2,?3)",
                    params![source, url, method],
                )?;
                consumed.insert(path.clone());
            }
        }
        for (path, value) in &records {
            if path.starts_with("archive-v1/observations/") {
                let id = value["id"].as_i64().context("observation ID missing")?;
                ensure!(
                    id > 0 && *path == format!("archive-v1/observations/{id}.json"),
                    "observation path mismatch"
                );
                let source = value_string(value, "source_id")?;
                let revision = value["revision_id"].as_str();
                let status = value_string(value, "status")?;
                ensure!(
                    matches!(
                        status,
                        "complete" | "partial" | "missing" | "deleted" | "forbidden" | "error"
                    ),
                    "unsupported observation status"
                );
                tx.execute("INSERT INTO observations(id,source_id,revision_id,observed_at,status,extractor,evidence,raw_digest) VALUES(?1,?2,?3,?4,?5,?6,?7,NULL)",params![id,source,revision,value["observed_at"].as_i64().context("observation timestamp missing")?,status,value_string(value,"extractor")?,serde_json::to_string(&value["evidence"])?])?;
                let edges: Vec<(String, String)> =
                    serde_json::from_value(value["relations"].clone())?;
                for (target, kind) in edges {
                    tx.execute(
                        "INSERT INTO relations VALUES(?1,?2,?3,?4)",
                        params![source, target, kind, id],
                    )?;
                }
                if let Some(raw) = value["raw_digest"].as_str() {
                    self.blob_path(raw)?;
                    tx.execute("INSERT INTO semantic_unavailable VALUES('observation',?1,?2,'raw_bytes_not_exported')",params![id.to_string(),serde_json::to_string(value)?])?;
                }
                tx.execute(
                    "INSERT INTO projection_events(kind,entity_id) VALUES('observation',?1)",
                    [id.to_string()],
                )?;
                consumed.insert(path.clone());
            }
        }
        tx.execute("UPDATE sources SET current_revision=(SELECT revision_id FROM observations WHERE source_id=sources.id AND revision_id IS NOT NULL ORDER BY CASE status WHEN 'complete' THEN 0 ELSE 1 END,id DESC LIMIT 1)",[])?;
        for (path, value) in &records {
            if path.starts_with("archive-v1/views/") {
                let view: crate::sources::View = serde_json::from_value(value.clone())?;
                ensure!(
                    *path == format!("archive-v1/views/{}.json", view.id()?),
                    "view digest/path mismatch"
                );
                crate::sources::save_view(&tx, &view)?;
                consumed.insert(path.clone());
            } else if path.starts_with("archive-v1/derivatives/") {
                let key = value_string(value, "derivation_key")?;
                self.blob_path(key)?;
                ensure!(
                    *path == format!("archive-v1/derivatives/{key}/manifest.json"),
                    "derivative path mismatch"
                );
                let kind = value_string(value, "kind")?;
                let has_text = value["manifest_exported"]
                    .as_bool()
                    .context("derivative export policy missing")?;
                ensure!(
                    has_text
                        == matches!(
                            kind,
                            "metadata" | "metadata_chunk" | "translation" | "spoken_text"
                        ),
                    "unsupported derivative text policy"
                );
                if has_text {
                    ensure!(
                        value["manifest"].is_object(),
                        "missing exported text manifest"
                    );
                }
                if kind == "translation" {
                    ensure!(
                        text_blobs.contains_key(key),
                        "missing exported translation text"
                    );
                    consumed.insert(format!("archive-v1/derivatives/{key}/translation.md"));
                }
                tx.execute("INSERT INTO semantic_derivatives(derivation_key,kind,manifest,text_digest) VALUES(?1,?2,?3,?4)",params![key,kind,serde_json::to_string(value)?,text_blobs.get(key)])?;
                tx.execute("INSERT INTO projection_events(kind,entity_id) VALUES('semantic_derivative',?1)",[key])?;
                consumed.insert(path.clone());
            } else if path.starts_with("archive-v1/artifacts/") {
                let id = value_string(value, "artifact_id")?;
                self.blob_path(id)?;
                ensure!(
                    *path == format!("archive-v1/artifacts/{id}.json"),
                    "artifact path mismatch"
                );
                tx.execute("INSERT INTO semantic_unavailable VALUES('artifact',?1,?2,'binary_bytes_not_exported')",params![id,serde_json::to_string(value)?])?;
                tx.execute(
                    "INSERT INTO projection_events(kind,entity_id) VALUES('semantic_artifact',?1)",
                    [id],
                )?;
                consumed.insert(path.clone());
            }
        }
        ensure!(
            consumed.len() == files.len() && files.keys().all(|k| consumed.contains(k)),
            "unknown or unconsumed semantic export files"
        );
        tx.commit()?;
        Ok(())
    }
}
#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
