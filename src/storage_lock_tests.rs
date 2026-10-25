//! F03/F20 #82: observe SQLite's real WAL file locks from another process.
use super::*;

fn assert_external_lock(path: &Path, expected: bool, phase: &str) {
    let script = r#"
import errno, fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR)
try:
    try:
        fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB, 512, 0x40000000)
        blocked = False
        fcntl.lockf(fd, fcntl.LOCK_UN, 512, 0x40000000)
    except OSError as error:
        if error.errno not in (errno.EACCES, errno.EAGAIN):
            raise
        blocked = True
    print('blocked=' + str(blocked))
    assert blocked == (sys.argv[2] == 'true'), 'SQLite file lock was lost'
finally:
    os.close(fd)
"#;
    let result = std::process::Command::new("/usr/bin/python3")
        .args(["-c", script])
        .arg(path)
        .arg(expected.to_string())
        .output()
        .expect("run independent fcntl oracle");
    assert!(
        result.status.success(),
        "independent lock oracle at {phase}: {} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn cold_second_connection_preserves_active_wal_file_locks() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Store::open(root.path().join("state"))?;
    let database = store.root().join("archive.sqlite");
    let mut first = store.connection()?;
    let transaction = first.transaction()?;
    let _: i64 = transaction.query_row("SELECT count(*) FROM sources", [], |r| r.get(0))?;
    assert_eq!(
        transaction.pragma_query_value(None, "journal_mode", |r| r.get::<_, String>(0))?,
        "wal"
    );
    assert_external_lock(&database, true, "before cold connection");
    // The pool is empty while the first connection is checked out.
    let second = store.connection()?;
    assert_external_lock(&database, true, "after cold connection");
    drop(second);
    transaction.commit()?;
    drop(first);
    drop(store);
    assert_external_lock(&database, false, "after all connections close");
    Ok(())
}

#[test]
fn database_preflight_still_rejects_symlinks_and_non_files() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("state");
    drop(Store::open(&path)?);
    let db = path.join("archive.sqlite");
    let real = path.join("real.sqlite");
    fs::rename(&db, &real)?;
    std::os::unix::fs::symlink(&real, &db)?;
    assert!(Store::open(&path).is_err());
    fs::remove_file(&db)?;
    fs::create_dir(&db)?;
    assert!(Store::open(&path).is_err());
    Ok(())
}
