//! DA09 bounded historical admission and publication-closure controls.
use anyhow::Result;
use serde_json::json;
use telegram_archive_bot::{activities::*, config::*, sources::*, storage::Store};

fn config(root: &std::path::Path, accounts: usize) -> Config {
    let mut c = Config {
        state_dir: root.join("state"),
        ..Config::default()
    };
    c.daily.enabled = true;
    c.daily.max_accounts = 500;
    c.daily.accounts = (1..=accounts)
        .map(|i| WatchAccount {
            key: format!("fixture-{i}"),
            user_id: Some(i.to_string()),
            ..WatchAccount::default()
        })
        .collect();
    c
}
fn event(w: &telegram_archive_bot::planner::DayPlan, id: usize) -> ActivityCapture {
    ActivityCapture {
        actor: w.account.clone(),
        handle: None,
        display_name: None,
        provider_event_id: Some(id.to_string()),
        kind: ActivityKind::Original,
        discovery: DiscoveryRun {
            provider: "x".into(),
            key: w.id.clone(),
            provenance: json!({"window_id":w.id}),
        },
        occurrence_key: id.to_string(),
        observation_key: id.to_string(),
        activity_time: TimeEvidence::Unknown {
            provenance: json!({}),
        },
        source_time: TimeEvidence::Unknown {
            provenance: json!({}),
        },
        retrieved_at: w.utc_end,
        provenance: json!({}),
        capture: Capture {
            identity: Identity::x(&id.to_string()).unwrap(),
            submitted_url: None,
            content: Some(Content {
                body: "original".into(),
                metadata: ContentMetadata::default(),
            }),
            status: FetchStatus::Complete,
            extractor: "fixture".into(),
            evidence: json!({}),
            raw_digest: None,
            relations: vec![],
        },
        view_policy: ViewPolicy {
            max_nodes: 10,
            max_depth: 3,
        },
    }
}

#[test]
fn bounded_historical_admission_never_certifies_an_unindexed_window() -> Result<()> {
    let root = tempfile::tempdir()?;
    let c = config(root.path(), 1);
    let store = Store::open(&c.state_dir)?;
    let w = store
        .backfill_daily(
            &c,
            "2026-03-08".parse()?,
            "2026-03-08".parse()?,
            1_790_000_000,
        )?
        .windows
        .remove(0);
    for i in 1..=70 {
        store.record_activity(&event(&w, i))?;
    }
    let conn = rusqlite::Connection::open(c.state_dir.join("archive.sqlite"))?;
    // Model migration of pre019 canonical content, not provider completion evidence.
    conn.execute("DELETE FROM reader_activity_windows", [])?;
    conn.execute("UPDATE daily_windows SET state='complete'", [])?;
    let tick = store.reader_tick(&c, 1)?;
    assert!(tick.admitted <= 128);
    let h = store
        .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_some())
        .unwrap();
    let export = store.reader_export(&c, &h.manifest_id)?;
    assert!(
        !export.manifest.collection_complete,
        "64-row admission is not the complete 70-activity corpus"
    );
    assert_eq!(export.activities.len(), 64);
    for _ in 0..8 {
        assert_eq!(store.reader_tick(&c, 1)?.errors, 0);
    }
    let h = store
        .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    assert_eq!(
        store.reader_export(&c, &h.manifest_id)?.activities.len(),
        70
    );
    Ok(())
}

#[test]
fn bounded_window_bytes_fail_visibly_without_publishing_a_truncated_head() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut c = config(root.path(), 1);
    c.reader.export_bytes = 16_384;
    let store = Store::open(&c.state_dir)?;
    let w = store
        .backfill_daily(
            &c,
            "2026-03-08".parse()?,
            "2026-03-08".parse()?,
            1_790_000_000,
        )?
        .windows
        .remove(0);
    let mut input = event(&w, 1);
    input.provenance = json!({"fixture":"x".repeat(20_000)});
    store.record_activity(&input)?;
    let tick = store.reader_tick(&c, 1)?;
    assert_eq!(
        tick.errors, 1,
        "window construction shares the explicit byte budget"
    );
    assert!(
        store
            .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
            .is_empty()
    );
    c.reader.export_bytes = 100_000;
    for _ in 0..5 {
        assert_eq!(store.reader_tick(&c, 1)?.errors, 0);
    }
    assert_eq!(
        store
            .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
            .len(),
        2
    );
    Ok(())
}

#[test]
fn bounded_hierarchical_account_index_waits_for_its_intermediate_pages() -> Result<()> {
    let root = tempfile::tempdir()?;
    let c = config(root.path(), 33);
    config(root.path(), 500).validate()?; // Public generic sizing example; no private watchlist.
    let store = Store::open(&c.state_dir)?;
    store.backfill_daily(
        &c,
        "2026-03-08".parse()?,
        "2026-03-08".parse()?,
        1_790_000_000,
    )?;
    for _ in 0..5 {
        assert_eq!(store.reader_tick(&c, 16)?.errors, 0);
    }
    let conn = rusqlite::Connection::open(c.state_dir.join("archive.sqlite"))?;
    // Synthetic, valid-shaped stored DA06 receipts isolate dependency propagation.
    // Actual subprocess readback is covered separately by reader.rs; these are not live proof.
    let heads = store.reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?;
    for (i, h) in heads.iter().filter(|h| h.window_id.is_some()).enumerate() {
        let record: String = conn.query_row(
            "SELECT record FROM notion_documents WHERE id=?1",
            [&h.manifest_id],
            |r| r.get(0),
        )?;
        let record: telegram_archive_bot::notion::Record = serde_json::from_str(&record)?;
        let plan = record.plan(&c.notion)?;
        let page = format!("00000000-0000-0000-0000-{:012}", i + 1);
        let receipt = telegram_archive_bot::notion::VerifiedPage {
            page_id: page.clone(),
            publication_key: plan.publication_key.clone(),
            canonical_digest: plan.canonical_digest.clone(),
            content_sha256: plan.content_sha256,
            schema_version: 1,
            destination_key: "fixture".into(),
            block_ids: vec![],
        };
        conn.execute("UPDATE notion_queue SET state='complete',page_id=?1,receipt=?2,expected_digest=?3 WHERE id=?4",rusqlite::params![page,serde_json::to_string(&receipt)?,plan.canonical_digest,format!("document:{}",h.manifest_id)])?;
    }
    for _ in 0..3 {
        assert_eq!(store.reader_tick(&c, 16)?.errors, 0);
    }
    let head = store
        .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    let export = store.reader_export(&c, &head.manifest_id)?;
    assert!(
        !export.manifest.publication_verified,
        "unpublished intermediate index pages prevent verified remote traversal"
    );
    assert_eq!(export.manifest.accounts.len(), 2);
    let count: u64 = conn.query_row(
        "SELECT count(*) FROM reader_dependencies WHERE scope=?1 AND kind='queue'",
        [&head.scope],
        |r| r.get(0),
    )?;
    assert_eq!(
        count, 35,
        "33 account roots plus 2 intermediate pages must dirty their parent on verification"
    );
    Ok(())
}

#[test]
fn partial_context_is_not_a_complete_body_even_when_root_is_complete() -> Result<()> {
    let root = tempfile::tempdir()?;
    let c = config(root.path(), 1);
    let store = Store::open(&c.state_dir)?;
    let w = store
        .backfill_daily(
            &c,
            "2026-03-08".parse()?,
            "2026-03-08".parse()?,
            1_790_000_000,
        )?
        .windows
        .remove(0);
    let mut context = event(&w, 2).capture;
    context.status = FetchStatus::Partial;
    context.content.as_mut().unwrap().body = "Only context preview".into();
    store.capture(&context)?;
    let mut input = event(&w, 1);
    input.capture.relations.push(Related {
        target: Identity::x("2")?,
        kind: RelationKind::Quotes,
    });
    store.record_activity(&input)?;
    for _ in 0..5 {
        assert_eq!(store.reader_tick(&c, 1)?.errors, 0);
    }
    let h = store
        .reader_heads("2026-03-08", "America/Los_Angeles", 100, None)?
        .into_iter()
        .find(|h| h.window_id.is_none())
        .unwrap();
    let export = store.reader_export(&c, &h.manifest_id)?;
    assert!(
        !export.manifest.body_complete,
        "complete root cannot promote a partial context preview"
    );
    assert_eq!(export.activities[0].disposition, "partial");
    Ok(())
}
