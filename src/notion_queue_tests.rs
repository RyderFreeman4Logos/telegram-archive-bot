//! #102: real SQLite selector transactions, without providers or process clocks.
use super::*;
use std::os::unix::fs::PermissionsExt;

fn fixture() -> (tempfile::TempDir, Store, rusqlite::Connection) {
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let store = Store::open(root.path()).unwrap();
    let db = rusqlite::Connection::open(root.path().join("archive.sqlite")).unwrap();
    (root, store, db)
}

fn enqueue(
    db: &rusqlite::Connection,
    id: &str,
    destination: &str,
    at: i64,
    unknown: bool,
    kind: &str,
) {
    db.execute("INSERT INTO notion_queue(id,kind,entity_id,created_at,destination_key,state,available_at,may_have_remote,attempts,create_attempts) VALUES(?1,?2,?1,1,?3,?4,?5,?6,?7,?7)",params![id,kind,destination,if unknown {"reconcile"} else {"ready"},at,unknown,if unknown {3} else {0}]).unwrap();
}

fn cursor(db: &rusqlite::Connection, destination: &str) -> Option<bool> {
    db.query_row(
        "SELECT delayed_next FROM notion_admission_cursor WHERE destination_key=?1",
        [destination],
        |r| r.get(0),
    )
    .optional()
    .unwrap()
}

fn queue_snapshot(db: &rusqlite::Connection) -> String {
    db.query_row("SELECT json_group_array(json_array(id,kind,entity_id,state,available_at,may_have_remote,attempts,create_attempts,record,publication_key,expected_digest,page_id,progress)) FROM (SELECT * FROM notion_queue ORDER BY id)", [], |r| r.get(0)).unwrap()
}

#[test]
fn lanes_resume_after_reopen_with_due_bounds_and_destination_isolation() {
    let (root, store, db) = fixture();
    let config = Notion::default();
    let at = now();
    enqueue(&db, "immediate", "first", -1, false, "revision");
    enqueue(&db, "due-first", "first", at - 10, true, "revision");
    enqueue(&db, "due-second", "first", at - 5, true, "revision");
    enqueue(&db, "future", "first", at + 3600, true, "revision");
    enqueue(&db, "exhausted", "first", 0, false, "revision");
    db.execute(
        "UPDATE notion_queue SET create_attempts=3 WHERE id='exhausted'",
        [],
    )
    .unwrap();
    enqueue(&db, "other-immediate", "other", 0, false, "revision");
    enqueue(&db, "other-delayed", "other", at - 1, true, "revision");
    assert_eq!(
        store.notion_next("first", &config).unwrap().unwrap().id,
        "due-first"
    );
    assert_eq!(cursor(&db, "first"), Some(false));
    db.execute(
        "UPDATE notion_queue SET available_at=?1 WHERE id='due-first'",
        [at + 3600],
    )
    .unwrap();
    assert_eq!(
        store.notion_next("first", &config).unwrap().unwrap().id,
        "immediate"
    );
    assert_eq!(cursor(&db, "first"), Some(true));
    db.execute(
        "UPDATE notion_queue SET state='complete' WHERE id='immediate'",
        [],
    )
    .unwrap();
    drop(store);
    let store = Store::open(root.path()).unwrap();
    assert_eq!(
        store.notion_next("first", &config).unwrap().unwrap().id,
        "due-second"
    );
    assert_eq!(
        store.notion_next("other", &config).unwrap().unwrap().id,
        "other-delayed"
    );
    assert_eq!(cursor(&db, "first"), Some(false));
    db.execute(
        "UPDATE notion_queue SET available_at=?1 WHERE id='due-second'",
        [at + 3600],
    )
    .unwrap();
    let snapshot = queue_snapshot(&db);
    assert!(store.notion_next("first", &config).unwrap().is_none());
    assert_eq!(cursor(&db, "first"), Some(false));
    assert_eq!(queue_snapshot(&db), snapshot);
}

#[test]
fn empty_hold_and_cursor_write_or_commit_failure_never_return_work() {
    let (_root, store, db) = fixture();
    let config = Notion::default();
    assert!(store.notion_next("fixture", &config).unwrap().is_none());
    assert_eq!(cursor(&db, "fixture"), None);
    enqueue(&db, "only", "fixture", 0, false, "revision");
    db.execute(
        "INSERT INTO operational_holds(name,reason) VALUES('fixture','recovery')",
        [],
    )
    .unwrap();
    assert!(store.notion_next("fixture", &config).unwrap().is_none());
    assert_eq!(cursor(&db, "fixture"), None);
    db.execute("DELETE FROM operational_holds", []).unwrap();
    let snapshot = queue_snapshot(&db);
    db.execute_batch("CREATE TRIGGER refuse_cursor_insert BEFORE INSERT ON notion_admission_cursor BEGIN SELECT RAISE(ABORT,'cursor insert denied'); END;").unwrap();
    assert!(store.notion_next("fixture", &config).is_err());
    assert_eq!(cursor(&db, "fixture"), None);
    db.execute_batch("DROP TRIGGER refuse_cursor_insert; INSERT INTO notion_admission_cursor VALUES('fixture',0); CREATE TRIGGER refuse_cursor_update BEFORE UPDATE ON notion_admission_cursor BEGIN SELECT RAISE(ABORT,'cursor update denied'); END;").unwrap();
    assert!(store.notion_next("fixture", &config).is_err());
    assert_eq!(cursor(&db, "fixture"), Some(false));
    // A deferred FK fails only at COMMIT, after the cursor statement succeeds.
    db.execute_batch("DROP TRIGGER refuse_cursor_update; CREATE TABLE fixture_deferred(target TEXT REFERENCES notion_admission_cursor(destination_key) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER refuse_cursor_commit AFTER UPDATE ON notion_admission_cursor BEGIN INSERT INTO fixture_deferred VALUES('missing'); END;").unwrap();
    assert!(store.notion_next("fixture", &config).is_err());
    assert_eq!(cursor(&db, "fixture"), Some(false));
    assert_eq!(
        db.query_row("SELECT count(*) FROM fixture_deferred", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        0
    );
    assert_eq!(queue_snapshot(&db), snapshot);
}

#[test]
fn selected_turn_is_shared_by_validation_and_pdf_without_create_authority() {
    let (_root, store, db) = fixture();
    let config = Notion {
        enabled: true,
        destination: Some(crate::config::NotionDestination::Page {
            id: "11111111-1111-1111-1111-111111111111".into(),
        }),
        ..Notion::default()
    };
    let destination = store.notion_configure(&config).unwrap().unwrap();
    store
        .capture(&crate::sources::Capture {
            identity: crate::sources::Identity::x("100").unwrap(),
            submitted_url: None,
            content: Some(crate::sources::Content {
                body: "synthetic retained body".into(),
                metadata: Default::default(),
            }),
            status: crate::sources::FetchStatus::Complete,
            extractor: "issue102-fixture".into(),
            evidence: json!({"fixture":true}),
            raw_digest: None,
            relations: vec![],
        })
        .unwrap();
    let snapshot = queue_snapshot(&db);
    let mut intent = store.notion_next(&destination, &config).unwrap().unwrap();
    assert_eq!(cursor(&db, &destination), Some(true));
    // Cancellation after selection can consume a turn, but selection alone
    // does not pin a record, increment an attempt or permit a remote write.
    assert_eq!(queue_snapshot(&db), snapshot);
    let record = store.notion_record(&intent, config.record_bytes).unwrap();
    db.execute(
        "INSERT INTO operational_holds(name,reason) VALUES('fixture','arrived after selection')",
        [],
    )
    .unwrap();
    assert!(
        store
            .notion_begin(&mut intent, &record, &config)
            .unwrap_err()
            .to_string()
            .contains("held after recovery")
    );
    assert_eq!(queue_snapshot(&db), snapshot);
    db.execute("DELETE FROM operational_holds", []).unwrap();
    store.notion_invalid_record(&intent).unwrap();
    enqueue(&db, "pdf", &destination, -1, true, "pdf_attachment");
    // Delayed lane is empty. Fallback must use the actual immediate lane, so
    // delayed remains preferred instead of blindly flipping its old value.
    let pdf = store.notion_next(&destination, &config).unwrap().unwrap();
    assert_eq!(pdf.kind, "pdf_attachment");
    assert_eq!(cursor(&db, &destination), Some(true));
    assert!(pdf.may_have_remote);
    assert_eq!(pdf.attempts, 3);
    assert!(pdf.record.is_none());
    assert_eq!(
        db.query_row("SELECT count(*) FROM notion_attempts", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn both_lane_queries_use_small_index_searches_in_a_large_queue() {
    let (_root, _store, db) = fixture();
    db.execute_batch("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<132000) INSERT INTO notion_queue(id,kind,entity_id,created_at,destination_key,state) SELECT printf('fixture-%06d',x),'revision',printf('entity-%06d',x),x,'fixture',CASE WHEN x<=40000 THEN 'complete' ELSE 'ready' END FROM n;").unwrap();
    enqueue(&db, "retry-due", "fixture", now() - 1, true, "revision");
    enqueue(
        &db,
        "retry-future",
        "fixture",
        now() + 3600,
        true,
        "revision",
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM notion_queue", [], |r| r
            .get::<_, u64>(0))
            .unwrap(),
        132002
    );
    for (lane, sql, expected) in [
        ("immediate", NEXT_IMMEDIATE_SQL, "fixture-040001"),
        ("delayed", NEXT_DELAYED_SQL, "retry-due"),
    ] {
        let at = now();
        let plan = db
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map(params!["fixture", at, 3], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
            .join("\n");
        assert!(
            plan.contains("SEARCH notion_queue USING INDEX notion_queue_due")
                && !plan.contains("TEMP B-TREE"),
            "{lane}: {plan}"
        );
        let mut statement = db.prepare(sql).unwrap();
        let id: String = statement
            .query_row(params!["fixture", at, 3], |r| r.get(0))
            .unwrap();
        assert_eq!(id, expected);
        let steps = statement.get_status(rusqlite::StatementStatus::VmStep);
        assert!(
            steps < 1000,
            "{lane}: {steps} VM steps for one ordered admission"
        );
        eprintln!("issue102 queue_rows=132002 lane={lane} vm_steps={steps} plan={plan}");
    }
}
