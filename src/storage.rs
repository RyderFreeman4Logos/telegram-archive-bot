use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const SCHEMA_VERSION: u32 = 14;

#[derive(Clone, Debug)]
pub struct Store {
    root: PathBuf,
    idle: Arc<Mutex<Vec<Connection>>>,
}

pub(crate) struct StoreConnection {
    connection: Option<Connection>,
    idle: Arc<Mutex<Vec<Connection>>>,
}

impl std::ops::Deref for StoreConnection {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.connection.as_ref().expect("live database connection")
    }
}
impl std::ops::DerefMut for StoreConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        self.connection.as_mut().expect("live database connection")
    }
}
impl Drop for StoreConnection {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take()
            && connection.is_autocommit()
            && let Ok(mut idle) = self.idle.lock()
            && idle.len() < 8
        {
            idle.push(connection);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blob {
    pub digest: String,
    pub size: u64,
}

pub(crate) fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(crate) fn private_dir(path: &Path) -> Result<()> {
    if !path.exists() {
        if let Some(parent) = path.parent() {
            ensure!(parent.is_dir(), "storage parent must already exist");
        }
        fs::DirBuilder::new().mode(0o700).create(path)?;
    }
    let meta = fs::symlink_metadata(path)?;
    ensure!(
        meta.is_dir() && !meta.file_type().is_symlink(),
        "managed directory is not a real directory"
    );
    ensure!(
        meta.uid() == unsafe { libc::geteuid() },
        "managed directory has another owner"
    );
    ensure!(
        meta.permissions().mode() & 0o077 == 0,
        "managed directory must have mode 0700"
    );
    Ok(())
}

fn regular_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    ensure!(
        file.metadata()?.is_file(),
        "managed object is not a regular file"
    );
    Ok(file)
}

pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

impl Store {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        ensure!(root.is_absolute(), "state directory must be absolute");
        private_dir(root)?;
        let store = Self {
            root: root.canonicalize()?,
            idle: Arc::default(),
        };
        for dir in ["blobs", "staging", "work"] {
            private_dir(&store.root.join(dir))?;
        }
        let _guard = store.publication_lock()?;
        let mut conn = store.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let version: u32 = tx.pragma_query_value(None, "user_version", |row| row.get(0))?;
        ensure!(
            version <= SCHEMA_VERSION,
            "database schema is newer than this binary"
        );
        if version == 0 {
            tx.execute_batch(include_str!("../migrations/001_canonical.sql"))?;
        }
        if version < 2 {
            tx.execute_batch(include_str!("../migrations/002_revision_quality.sql"))?;
        }
        if version < 3 {
            tx.execute_batch(include_str!("../migrations/003_ingress.sql"))?;
        }
        if version < 4 {
            tx.execute_batch(include_str!("../migrations/004_job_recovery.sql"))?;
        }
        if version < 5 {
            tx.execute_batch(include_str!("../migrations/005_derivatives.sql"))?;
        }
        if version < 6 {
            tx.execute_batch(include_str!("../migrations/006_immutable_artifacts.sql"))?;
        }
        if version < 7 {
            tx.execute_batch(include_str!("../migrations/007_delivery_attempts.sql"))?;
        }
        if version < 8 {
            tx.execute_batch(include_str!("../migrations/008_projection.sql"))?;
        }
        if version < 9 {
            tx.execute_batch(include_str!("../migrations/009_recovery.sql"))?;
        }
        if version < 10 {
            tx.execute_batch(include_str!("../migrations/010_transforms.sql"))?;
        }
        if version < 11 {
            tx.execute_batch(include_str!("../migrations/011_message_origins.sql"))?;
        }
        if version < 12 {
            tx.execute_batch(include_str!("../migrations/012_observation_source.sql"))?;
        }
        if version < 13 {
            tx.execute_batch(include_str!("../migrations/013_account_activities.sql"))?;
        }
        if version < 14 {
            tx.execute_batch(include_str!("../migrations/014_daily_planner.sql"))?;
        }
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        tx.commit()?;
        sync_dir(&store.root)?;
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn connection(&self) -> Result<StoreConnection> {
        let cached = self
            .idle
            .lock()
            .map_err(|_| anyhow::anyhow!("database connection pool poisoned"))?
            .pop();
        let connection = match cached {
            Some(connection) => connection,
            None => self.open_connection()?,
        };
        Ok(StoreConnection {
            connection: Some(connection),
            idle: self.idle.clone(),
        })
    }

    fn open_connection(&self) -> Result<Connection> {
        let path = self.root.join("archive.sqlite");
        if path.symlink_metadata().is_ok() {
            regular_file(&path)?;
        } else {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)?;
        }
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.pragma_update(None, "foreign_keys", true)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        Ok(conn)
    }

    // The OS releases this fence on process death; publishers cannot resume after losing it.
    pub(crate) fn publication_lock(&self) -> Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(self.root.join("publication.lock"))?;
        file.lock_exclusive()?;
        Ok(file)
    }

    pub fn blob_path(&self, digest: &str) -> Result<PathBuf> {
        ensure!(
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid blob digest"
        );
        Ok(self.root.join("blobs").join(digest))
    }

    pub fn publish_bytes(&self, bytes: &[u8]) -> Result<Blob> {
        self.publish_reader(bytes, bytes.len() as u64)
    }

    pub fn publish_reader(&self, input: impl Read, max_bytes: u64) -> Result<Blob> {
        self.publish_inner(input, max_bytes, |_| Ok(()))
    }

    fn publish_inner(
        &self,
        input: impl Read,
        max_bytes: u64,
        checkpoint: impl Fn(&str) -> Result<()>,
    ) -> Result<Blob> {
        use sha2::{Digest, Sha256};
        let _guard = self.publication_lock()?;
        let mut stage = tempfile::NamedTempFile::new_in(self.root.join("staging"))?;
        let mut hasher = Sha256::new();
        let mut input = input.take(max_bytes.saturating_add(1));
        let mut size = 0u64;
        let mut buf = [0u8; 65536];
        loop {
            let count = input.read(&mut buf)?;
            if count == 0 {
                break;
            }
            size += count as u64;
            ensure!(size <= max_bytes, "blob exceeds byte limit");
            hasher.update(&buf[..count]);
            stage.write_all(&buf[..count])?;
        }
        stage.as_file().sync_all()?;
        checkpoint("staged")?;
        let blob = Blob {
            digest: format!("{:x}", hasher.finalize()),
            size,
        };
        let path = self.blob_path(&blob.digest)?;
        match fs::hard_link(stage.path(), &path) {
            Ok(()) => sync_dir(&self.root.join("blobs"))?,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => self.verify_file(&blob)?,
            Err(e) => return Err(e.into()),
        }
        checkpoint("published")?;
        self.connection()?.execute("INSERT INTO blobs(digest,size,created_at) VALUES(?1,?2,?3) ON CONFLICT(digest) DO NOTHING", params![blob.digest, blob.size, now()])?;
        self.verify_blob(&blob.digest)?;
        checkpoint("catalogued")?;
        Ok(blob)
    }

    fn verify_file(&self, blob: &Blob) -> Result<()> {
        use sha2::{Digest, Sha256};
        let mut file = regular_file(&self.blob_path(&blob.digest)?)
            .with_context(|| format!("missing or unsafe blob {}", blob.digest))?;
        ensure!(
            file.metadata()?.len() == blob.size,
            "blob {} has wrong size",
            blob.digest
        );
        let mut hasher = Sha256::new();
        std::io::copy(&mut file, &mut hasher)?;
        ensure!(
            format!("{:x}", hasher.finalize()) == blob.digest,
            "corrupt blob {}",
            blob.digest
        );
        Ok(())
    }

    pub fn verify_blob(&self, digest: &str) -> Result<Blob> {
        let size: Option<u64> = self
            .connection()?
            .query_row("SELECT size FROM blobs WHERE digest=?1", [digest], |r| {
                r.get(0)
            })
            .optional()?;
        let blob = Blob {
            digest: digest.to_owned(),
            size: size.context("blob is not catalogued")?,
        };
        self.verify_file(&blob)?;
        Ok(blob)
    }

    pub fn read_blob(&self, digest: &str, max_bytes: u64) -> Result<Vec<u8>> {
        let _guard = self.publication_lock()?;
        let blob = self.verify_blob(digest)?;
        ensure!(blob.size <= max_bytes, "blob exceeds read limit");
        Ok(fs::read(self.blob_path(digest)?)?)
    }

    pub fn integrity(&self) -> Result<usize> {
        let _guard = self.publication_lock()?;
        let conn = self.connection()?;
        let integrity: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        ensure!(integrity == "ok", "database integrity check failed");
        ensure!(
            !conn.prepare("PRAGMA foreign_key_check")?.exists([])?,
            "foreign key check failed"
        );
        let blobs = conn
            .prepare("SELECT digest,size FROM blobs ORDER BY digest")?
            .query_map([], |r| {
                Ok(Blob {
                    digest: r.get(0)?,
                    size: r.get(1)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for blob in &blobs {
            self.verify_file(blob)?;
        }
        Ok(blobs.len())
    }

    // Catalogued objects are retained; only uncommitted files and abandoned stages are collected.
    pub fn gc(&self, grace: Duration) -> Result<usize> {
        let _guard = self.publication_lock()?;
        let conn = self.connection()?;
        let mut removed = 0;
        for dir in ["staging", "blobs"] {
            for entry in fs::read_dir(self.root.join(dir))? {
                let entry = entry?;
                let meta = entry.path().symlink_metadata()?;
                ensure!(
                    meta.is_file() && !meta.file_type().is_symlink(),
                    "unsafe file in managed object directory"
                );
                if meta.modified()?.elapsed().unwrap_or_default() < grace {
                    continue;
                }
                if dir == "blobs" {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    self.blob_path(&name)?;
                    if conn
                        .query_row("SELECT 1 FROM blobs WHERE digest=?1", [name], |_| Ok(()))
                        .optional()?
                        .is_some()
                    {
                        continue;
                    }
                }
                fs::remove_file(entry.path())?;
                removed += 1;
            }
            sync_dir(&self.root.join(dir))?;
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest;
    use anyhow::bail;
    use std::sync::{Arc, Barrier};

    #[test]
    fn idle_store_keeps_wal_open_without_retaining_abandoned_transactions() -> Result<()> {
        let temp = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let store = Store::open(temp.path())?;
        let blob = store.publish_bytes(b"Durable original")?;
        let wal = temp.path().join("archive.sqlite-wal");
        assert!(
            wal.exists(),
            "idle operations must not repeatedly checkpoint and delete the WAL"
        );
        {
            let conn = store.connection()?;
            conn.execute_batch("BEGIN IMMEDIATE; INSERT INTO polling_cursors VALUES(99,123);")?;
            // An abandoned raw transaction must be closed, not handed to another worker.
        }
        let cloned = store.clone();
        let conn = cloned.connection()?;
        assert_eq!(
            conn.query_row("SELECT count(*) FROM polling_cursors", [], |r| r
                .get::<_, u64>(0))?,
            0
        );
        assert_eq!(
            conn.pragma_query_value(None, "synchronous", |r| r.get::<_, u32>(0))?,
            2
        );
        assert_eq!(
            conn.pragma_query_value(None, "foreign_keys", |r| r.get::<_, u32>(0))?,
            1
        );
        drop(conn);
        drop(store);
        assert_eq!(cloned.read_blob(&blob.digest, 100)?, b"Durable original");
        drop(cloned);
        let reopened = Store::open(temp.path())?;
        assert_eq!(reopened.integrity()?, 1);
        Ok(())
    }

    #[test]
    fn migrations_constraints_and_concurrent_publication() -> Result<()> {
        let temp = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let store = Store::open(temp.path())?;
        let handles = (0..8)
            .map(|_| {
                let store = store.clone();
                std::thread::spawn(move || store.publish_bytes(b"same immutable bytes"))
            })
            .collect::<Vec<_>>();
        for h in handles {
            h.join().unwrap()?;
        }
        assert_eq!(store.integrity()?, 1);
        let conn = store.connection()?;
        assert_eq!(
            conn.pragma_query_value(None, "foreign_keys", |r| r.get::<_, u32>(0))?,
            1
        );
        assert_eq!(
            conn.pragma_query_value(None, "synchronous", |r| r.get::<_, u32>(0))?,
            2
        );
        assert!(conn.execute("INSERT INTO observations(source_id,observed_at,status,extractor,evidence) VALUES('absent',0,'missing','test','{}')", []).is_err());
        assert!(
            conn.execute("INSERT INTO blobs VALUES('short',1,0)", [])
                .is_err()
        );
        drop(conn);
        assert_eq!(Store::open(temp.path())?.integrity()?, 1);
        Ok(())
    }

    #[test]
    fn interrupted_publication_recovers_without_missing_ready_bytes() -> Result<()> {
        for stop in ["staged", "published", "catalogued"] {
            let temp = tempfile::Builder::new()
                .permissions(fs::Permissions::from_mode(0o700))
                .tempdir()?;
            let store = Store::open(temp.path())?;
            let result = store.publish_inner(&b"crash fixture"[..], 100, |phase| {
                if phase == stop {
                    bail!("injected failure");
                }
                Ok(())
            });
            assert!(result.is_err());
            assert_eq!(store.integrity()?, usize::from(stop == "catalogued"));
            store.gc(Duration::ZERO)?;
            let blob = store.publish_bytes(b"crash fixture")?;
            assert_eq!(store.read_blob(&blob.digest, 100)?, b"crash fixture");
        }
        Ok(())
    }

    #[test]
    fn crash_child() -> Result<()> {
        let Some(root) = std::env::var_os("ARCHIVE_TEST_CRASH_ROOT") else {
            return Ok(());
        };
        let stop = std::env::var("ARCHIVE_TEST_CRASH_PHASE")?;
        Store::open(root)?.publish_inner(&b"process death fixture"[..], 100, |phase| {
            if phase == stop {
                std::process::exit(91);
            }
            Ok(())
        })?;
        bail!("crash checkpoint was not reached")
    }

    #[test]
    fn process_death_leaves_only_recoverable_files() -> Result<()> {
        for phase in ["staged", "published", "catalogued"] {
            let temp = tempfile::Builder::new()
                .permissions(fs::Permissions::from_mode(0o700))
                .tempdir()?;
            let output = std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "storage::tests::crash_child"])
                .env("ARCHIVE_TEST_CRASH_ROOT", temp.path())
                .env("ARCHIVE_TEST_CRASH_PHASE", phase)
                .output()?;
            assert_eq!(
                output.status.code(),
                Some(91),
                "phase={phase}; stdout={}; stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let store = Store::open(temp.path())?;
            assert_eq!(store.integrity()?, usize::from(phase == "catalogued"));
            assert!(store.gc(Duration::ZERO)? >= usize::from(phase != "catalogued"));
            store.publish_bytes(b"process death fixture")?;
            assert_eq!(store.integrity()?, 1);
        }
        Ok(())
    }

    #[test]
    fn gc_cannot_race_publisher_and_corruption_is_never_a_cache_hit() -> Result<()> {
        let temp = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let store = Store::open(temp.path())?;
        let barrier = Arc::new(Barrier::new(2));
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = {
            let s = store.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                s.publish_inner(&b"pinned bytes"[..], 100, |phase| {
                    if phase == "published" {
                        b.wait();
                        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
                    }
                    Ok(())
                })
            })
        };
        barrier.wait();
        let collector = {
            let s = store.clone();
            std::thread::spawn(move || {
                let r = s.gc(Duration::ZERO);
                let _ = tx.send(());
                r
            })
        };
        let blob = worker.join().unwrap()?;
        assert_eq!(collector.join().unwrap()?, 0);
        fs::write(store.blob_path(&blob.digest)?, b"bad")?;
        assert!(store.verify_blob(&blob.digest).is_err());
        assert!(store.publish_bytes(b"pinned bytes").is_err());
        assert!(store.integrity().is_err());
        assert!(store.blob_path("../../etc/passwd").is_err());
        Ok(())
    }

    #[test]
    fn rejects_oversize_and_symlinked_objects() -> Result<()> {
        let temp = tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let store = Store::open(temp.path())?;
        assert!(store.publish_reader(&b"too long"[..], 3).is_err());
        let hash = digest(b"symlink");
        std::os::unix::fs::symlink("/etc/passwd", store.blob_path(&hash)?)?;
        assert!(store.publish_bytes(b"symlink").is_err());
        Ok(())
    }
}
