use anyhow::Result;
use serde_json::json;
use std::{fs::Permissions, os::unix::fs::PermissionsExt};
use telegram_archive_bot::{sources::*, storage::Store};

fn fixture(id: &str, body: &str) -> Capture {
    Capture {
        identity: Identity::x(id).unwrap(),
        submitted_url: Some(format!("https://twitter.com/any/status/{id}?s=20")),
        content: Some(Content {
            body: body.into(),
            metadata: ContentMetadata::default(),
        }),
        status: FetchStatus::Complete,
        extractor: "synthetic-v1".into(),
        evidence: json!({"likes":1}),
        raw_digest: None,
        relations: vec![],
    }
}

fn temp() -> Result<tempfile::TempDir> {
    Ok(tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir()?)
}

#[test]
fn abc_all_submission_orders_reuse_revisions() -> Result<()> {
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let dir = temp()?;
        let store = Store::open(dir.path())?;
        let a = fixture("100", "A complete original");
        let mut b = fixture("200", "B quotes A");
        b.relations.push(Related {
            target: a.identity.clone(),
            kind: RelationKind::Quotes,
        });
        let mut c = fixture("300", "C replies to A");
        c.relations.push(Related {
            target: a.identity.clone(),
            kind: RelationKind::RepliesTo,
        });
        let captures = [a, b, c];
        for i in order {
            store.capture(&captures[i])?;
        }
        for i in order {
            store.capture(&captures[i])?;
        }
        assert_eq!(store.source_counts()?, (3, 3, 6));
        let policy = ViewPolicy {
            max_nodes: 10,
            max_depth: 10,
        };
        for (index, kind) in [(1, "quotes"), (2, "replies_to")] {
            let view = store.create_view(&captures[index].identity, policy.clone())?;
            assert_eq!(view.members.len(), 2);
            assert_eq!(view.edges.len(), 1);
            assert_eq!(view.edges[0].kind, kind);
            assert!(view.members.iter().all(|m| m.revision_id.is_some()));
            assert_eq!(store.view(&view.id()?)?, view);
            assert_eq!(
                store.create_view(&captures[index].identity, policy.clone())?,
                view
            );
        }
    }
    Ok(())
}

#[test]
fn identities_preserve_semantic_queries_and_hash_routes() -> Result<()> {
    let expected = Identity::x("12345")?;
    for url in [
        "https://x.com/alice/status/12345",
        "https://twitter.com/bob/status/12345?s=20",
        "https://mobile.twitter.com/i/status/12345/photo/1",
    ] {
        assert_eq!(Identity::from_url(url)?, expected);
    }
    assert_ne!(
        Identity::from_url("https://example.org/?s=first")?,
        Identity::from_url("https://example.org/?s=second")?
    );
    assert_ne!(
        Identity::from_url("https://example.org/#/first")?,
        Identity::from_url("https://example.org/#/second")?
    );
    assert!(Identity::from_url("file:///etc/passwd").is_err());
    assert!(Identity::from_url("https://user:pass@example.org").is_err());
    Ok(())
}

#[test]
fn observations_previews_deletion_and_edits_preserve_pinned_views() -> Result<()> {
    let dir = temp()?;
    let store = Store::open(dir.path())?;
    let mut a = fixture("100", "A complete original");
    let a1 = store.capture(&a)?.unwrap();
    let view = store.create_view(
        &a.identity,
        ViewPolicy {
            max_nodes: 10,
            max_depth: 10,
        },
    )?;
    a.evidence = json!({"likes":999,"fetch_timestamp":"changed"});
    assert_eq!(store.capture(&a)?, Some(a1.clone()));
    a.status = FetchStatus::Partial;
    a.content.as_mut().unwrap().body = "short preview".into();
    assert_eq!(store.capture(&a)?, Some(a1.clone()));
    a.status = FetchStatus::Deleted;
    a.content = None;
    assert_eq!(store.capture(&a)?, Some(a1.clone()));
    assert_eq!(store.source_counts()?, (1, 1, 4));
    a.status = FetchStatus::Complete;
    a.content = Some(Content {
        body: "A edited full text".into(),
        metadata: ContentMetadata::default(),
    });
    let a2 = store.capture(&a)?.unwrap();
    assert_ne!(a1, a2);
    assert_eq!(
        store.view(&view.id()?)?.members[0].revision_id,
        Some(a1.clone())
    );
    assert_eq!(store.revision(&a1)?.content.body, "A complete original");
    assert_eq!(store.current_revision(&a.identity)?, Some(a2));
    Ok(())
}

#[test]
fn missing_stubs_hydration_cycles_and_budgets_are_explicit() -> Result<()> {
    let dir = temp()?;
    let store = Store::open(dir.path())?;
    let mut a = fixture("100", "A");
    let mut b = fixture("200", "B");
    b.relations.push(Related {
        target: a.identity.clone(),
        kind: RelationKind::Quotes,
    });
    store.capture(&b)?;
    let old = store.create_view(
        &b.identity,
        ViewPolicy {
            max_nodes: 10,
            max_depth: 10,
        },
    )?;
    assert_eq!(
        old.members[1].missing_reason.as_deref(),
        Some("unfetched_stub")
    );
    a.relations.push(Related {
        target: b.identity.clone(),
        kind: RelationKind::RepliesTo,
    });
    store.capture(&a)?;
    let hydrated = store.create_view(
        &b.identity,
        ViewPolicy {
            max_nodes: 10,
            max_depth: 10,
        },
    )?;
    assert_eq!(hydrated.members.len(), 2);
    assert_eq!(hydrated.edges.len(), 2);
    assert!(hydrated.members.iter().all(|m| m.missing_reason.is_none()));
    assert_eq!(store.view(&old.id()?)?, old);
    let limited = store.create_view(
        &b.identity,
        ViewPolicy {
            max_nodes: 1,
            max_depth: 10,
        },
    )?;
    assert_eq!(limited.members.len(), 1);
    assert_eq!(limited.omissions[0].reason, "node_limit");
    let shallow = store.create_view(
        &b.identity,
        ViewPolicy {
            max_nodes: 10,
            max_depth: 0,
        },
    )?;
    assert_eq!(shallow.omissions[0].reason, "depth_limit");
    Ok(())
}

#[test]
fn concurrent_duplicate_captures_converge() -> Result<()> {
    let dir = temp()?;
    let store = Store::open(dir.path())?;
    let handles = (0..8)
        .map(|_| {
            let s = store.clone();
            std::thread::spawn(move || s.capture(&fixture("100", "A")))
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap()?;
    }
    assert_eq!(store.source_counts()?, (1, 1, 8));
    Ok(())
}

#[test]
fn schema_one_upgrade_preserves_existing_content() -> Result<()> {
    let dir = temp()?;
    {
        let conn = rusqlite::Connection::open(dir.path().join("archive.sqlite"))?;
        conn.execute_batch(include_str!("../migrations/001_canonical.sql"))?;
        conn.execute_batch("PRAGMA user_version=1; INSERT INTO sources(id,provider,external_id) VALUES('legacy-schema-fixture','web','https://example.org/');")?;
    }
    let store = Store::open(dir.path())?;
    assert_eq!(store.source_counts()?, (1, 0, 0));
    assert_eq!(store.integrity()?, 0);
    let conn = rusqlite::Connection::open(dir.path().join("archive.sqlite"))?;
    assert_eq!(
        conn.pragma_query_value(None, "user_version", |r| r.get::<_, u32>(0))?,
        telegram_archive_bot::storage::SCHEMA_VERSION
    );
    Ok(())
}

#[test]
fn schema_eleven_upgrade_indexes_source_lookups_without_changing_results() -> Result<()> {
    let dir = temp()?;
    let store = Store::open(dir.path())?;
    let complete = fixture("100", "Complete original");
    store.capture(&complete)?;
    let mut partial = fixture("100", "Later partial preview");
    partial.status = FetchStatus::Partial;
    store.capture(&partial)?;
    store.capture(&fixture("200", "Unrelated later observation"))?;
    drop(store);
    let current_database = dir.path().join("archive.sqlite");
    let old = temp()?;
    let database = old.path().join("archive.sqlite");
    let conn = rusqlite::Connection::open(&database)?;
    std::fs::set_permissions(&database, Permissions::from_mode(0o600))?;
    // Build the historical schema itself; later migrations must not leak into this fixture.
    for migration in [
        include_str!("../migrations/001_canonical.sql"),
        include_str!("../migrations/002_revision_quality.sql"),
        include_str!("../migrations/003_ingress.sql"),
        include_str!("../migrations/004_job_recovery.sql"),
        include_str!("../migrations/005_derivatives.sql"),
        include_str!("../migrations/006_immutable_artifacts.sql"),
        include_str!("../migrations/007_delivery_attempts.sql"),
        include_str!("../migrations/008_projection.sql"),
        include_str!("../migrations/009_recovery.sql"),
        include_str!("../migrations/010_transforms.sql"),
        include_str!("../migrations/011_message_origins.sql"),
    ] {
        conn.execute_batch(migration)?;
    }
    conn.execute(
        "ATTACH DATABASE ?1 AS fixture_current",
        [current_database.to_string_lossy().as_ref()],
    )?;
    conn.execute_batch("BEGIN; PRAGMA defer_foreign_keys=ON;")?;
    for table in [
        "sources",
        "source_revisions",
        "observations",
        "source_aliases",
        "relations",
    ] {
        conn.execute_batch(&format!(
            "INSERT INTO {table} SELECT * FROM fixture_current.{table}"
        ))?;
    }
    conn.execute_batch("COMMIT; DETACH DATABASE fixture_current; PRAGMA user_version=11;")?;
    let dir = old;
    let queries = [
        "SELECT status FROM observations WHERE source_id=?1 ORDER BY id DESC LIMIT 1",
        "SELECT revision_id FROM observations WHERE source_id=?1 AND revision_id IS NOT NULL ORDER BY CASE status WHEN 'complete' THEN 0 ELSE 1 END,id DESC LIMIT 1",
    ];
    let identity = complete.identity.id();
    let before = queries
        .iter()
        .map(|query| conn.query_row(query, [&identity], |row| row.get::<_, String>(0)))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert_eq!(before[0], "partial");
    let preferred: String = conn.query_row(
        "SELECT current_revision FROM sources WHERE id=?1",
        [&identity],
        |row| row.get(0),
    )?;
    assert_eq!(before[1], preferred);
    let observations: String = conn.query_row(
        "SELECT json_group_array(json_array(id,source_id,revision_id,observed_at,status,extractor,evidence,raw_digest)) FROM (SELECT * FROM observations ORDER BY id)",
        [],
        |row| row.get(0),
    )?;
    drop(conn);
    let store = Store::open(dir.path())?;
    assert_eq!(store.source_counts()?, (2, 2, 3));
    assert_eq!(store.integrity()?, 0);
    let conn = rusqlite::Connection::open(&database)?;
    for (query, expected) in queries.iter().zip(before) {
        assert_eq!(
            conn.query_row(query, [&identity], |row| row.get::<_, String>(0))?,
            expected
        );
        let details = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {query}"))?
            .query_map([&identity], |row| row.get::<_, String>(3))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert!(
            details
                .iter()
                .any(|line| line.contains("SEARCH observations USING INDEX")),
            "{details:?}"
        );
    }
    assert_eq!(
        conn.query_row("SELECT json_group_array(json_array(id,source_id,revision_id,observed_at,status,extractor,evidence,raw_digest)) FROM (SELECT * FROM observations ORDER BY id)", [], |row| row.get::<_, String>(0))?,
        observations
    );
    Ok(())
}
