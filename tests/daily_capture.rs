//! DA04 / #71: opt-in capture-only configuration; no provider traffic.
use anyhow::Context;
use serde_json::json;
use telegram_archive_bot::config::Config;
use telegram_archive_bot::{activities::*, sources::*, storage::Store};

fn private_temp() -> anyhow::Result<tempfile::TempDir> {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir()?;
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))?;
    Ok(dir)
}

fn discovery(actor: &str, source: &str, day: &str) -> ActivityCapture {
    ActivityCapture {
        actor: AccountIdentity::ProviderId {
            provider: "x".into(),
            user_id: actor.into(),
        },
        handle: None,
        display_name: None,
        provider_event_id: Some(format!("event-{actor}-{source}-{day}")),
        kind: ActivityKind::Original,
        discovery: DiscoveryRun {
            provider: "x".into(),
            key: day.into(),
            provenance: json!({"utc_start":100,"utc_end":200}),
        },
        occurrence_key: format!("{actor}-{source}"),
        observation_key: "timeline-1".into(),
        activity_time: TimeEvidence::Unknown {
            provenance: json!({"fixture":true}),
        },
        source_time: TimeEvidence::Unknown {
            provenance: json!({"fixture":true}),
        },
        retrieved_at: 200,
        provenance: json!({"fixture":true}),
        capture: Capture {
            identity: Identity::x(source).unwrap(),
            submitted_url: None,
            content: None,
            status: FetchStatus::Missing,
            extractor: "timeline-fixture".into(),
            evidence: json!({"reason":"awaiting_body_hydration"}),
            raw_digest: None,
            relations: vec![],
        },
        view_policy: ViewPolicy {
            max_nodes: 32,
            max_depth: 4,
        },
    }
}
#[test]
fn da04_capture_only_configuration_is_explicit_and_bounded() {
    let config: Config = toml::from_str("[daily]\nenabled=true\n[daily.capture]\nenabled=true\nmax_admissions=100\nmax_links=8\nrefresh_seconds=86400\nretry_seconds=300\n").expect("DA04 capture-only configuration must be accepted");
    config.validate().unwrap();
    let invalid: Config = toml::from_str("[daily.capture]\nmax_links=1000000\n").unwrap();
    assert!(invalid.validate().is_err());
}

#[test]
fn da04_admission_has_a_durable_bounded_cursor_and_shared_source_work() -> anyhow::Result<()> {
    let root = private_temp()?;
    let store = Store::open(root.path())?;
    let mut config = Config {
        state_dir: root.path().into(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    let a = store.record_activity(&discovery("101", "10", "day1"))?;
    store.record_activity(&discovery("202", "10", "day2"))?;
    store.record_activity(&discovery("101", "11", "day1"))?;
    assert_eq!(store.admit_activity_captures(&config, 1)?, 1);
    assert_eq!(store.admit_activity_captures(&config, 1)?, 1);
    assert_eq!(store.admit_activity_captures(&config, 1)?, 1);
    assert_eq!(store.admit_activity_captures(&config, 1)?, 0);
    let conn = rusqlite::Connection::open(root.path().join("archive.sqlite"))?;
    assert_eq!(
        conn.query_row("SELECT count(*) FROM daily_capture_work", [], |r| r
            .get::<_, i64>(0))?,
        2
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM daily_capture_origins", [], |r| r
            .get::<_, i64>(0))?,
        3
    );
    assert_eq!(store.activity_observation(&a.id)?, a);
    Ok(())
}

#[test]
fn da04_entity_links_are_normalized_and_media_gaps_are_explicit() -> anyhow::Result<()> {
    let post = telegram_archive_bot::x::parse_fx(
        "10",
        &json!({"status":{"id":"10","text":"Original post","raw_text":{"facets":[{"type":"url","replacement":"https://example.org/article?utm_source=x&b=2#a"},{"type":"url","replacement":"https://example.org/article?b=2"}]},"media":{"videos":[{"url":"https://example.org/video.mp4"}]}}}),
    )?;
    assert_eq!(
        post.relations
            .iter()
            .filter(|r| r.kind == RelationKind::Links)
            .count(),
        1
    );
    assert_eq!(
        post.relations
            .iter()
            .find(|r| r.kind == RelationKind::Links)
            .unwrap()
            .target
            .url(),
        "https://example.org/article?b=2"
    );
    assert_eq!(
        post.evidence["media_text_status"],
        "references_only_not_transcribed"
    );
    Ok(())
}

struct FixtureCollector {
    calls: std::sync::Mutex<std::collections::BTreeMap<String, usize>>,
}
impl FixtureCollector {
    fn new() -> Self {
        Self {
            calls: Default::default(),
        }
    }
    fn count(&self, id: &str) -> usize {
        *self.calls.lock().unwrap().get(id).unwrap_or(&0)
    }
}
impl telegram_archive_bot::daily_capture::Collector for FixtureCollector {
    fn collect<'a>(
        &'a self,
        store: &'a Store,
        config: &'a Config,
        source: &'a Identity,
        cancel: &'a tokio_util::sync::CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<Vec<Capture>>> {
        use futures_util::FutureExt;
        async move {
   *self.calls.lock().unwrap().entry(source.external_id.clone()).or_default()+=1;
   if source.provider=="web" {
    let (mime,bytes)=if source.external_id.ends_with(".pdf") {("application/pdf",fixture_pdf("Retained PDF source text"))}else{("text/html",format!("<html><body><article><h1>External source</h1><p>{}</p></article></body></html>","Useful original external article text. ".repeat(70)).into_bytes())};
    let extracted=telegram_archive_bot::documents::Documents{store:store.clone(),config:config.clone(),http:Default::default()}.response(telegram_archive_bot::network::Response{retry_after_seconds:None,submitted:source.url().parse()?,resolved:source.url().parse()?,redirects:vec![],status:200,content_type:mime.into(),charset:None,bytes},cancel).await?;
    return Ok(vec![extracted.capture]);
   }
   let payload=match source.external_id.as_str(){
    "10"=>json!({"status":{"id":"10","text":"Original complete root post","author":{"id":"101"},"created_at":"1970-01-01T00:02:30Z","quote":{"id":"20"},"replying_to":{"status":"30"},"raw_text":{"facets":[{"type":"url","replacement":"https://example.org/article?utm_source=x#fragment"},{"type":"url","replacement":"https://example.org/article"},{"type":"url","replacement":"https://example.org/paper.pdf"}]}}}),
    "11"=>json!({"status":{"id":"11","text":"Known same-author continuation","author":{"id":"101"},"created_at":"1970-01-01T00:03:30Z","replying_to":{"status":"10"}}}),
    "20"=>json!({"status":{"id":"20","text":"Article post","author":{"id":"202"},"created_at":"1970-01-01T00:00:10Z","article":{"title":"Full Article","content":{"blocks":[{"type":"unstyled","text":"Full retained Article body"},{"type":"atomic"}]}},"quote":{"id":"40","text":"Useful embedded unavailable quote excerpt"}}}),
    "30"=>json!({"status":{"id":"30","text":"Ancestor outside selected day","created_at":"1970-01-01T00:00:20Z"}}),
    _=>json!({"code":404}),
   };
   let post=telegram_archive_bot::x::parse_fx(&source.external_id,&payload)?;
   let raw=store.publish_bytes(&serde_json::to_vec(&payload)?)?.digest;
   let mut captures=vec![Capture{identity:source.clone(),submitted_url:Some(source.url()),content:post.content,status:post.status,extractor:"fxembed-api-v2".into(),evidence:post.evidence,raw_digest:Some(raw),relations:post.relations}];
   for (id,body) in post.previews {captures.push(Capture{identity:Identity::x(&id)?,submitted_url:None,content:Some(body),status:FetchStatus::Partial,extractor:"embedded-quote-preview".into(),evidence:json!({"limitation":"excerpt_only"}),raw_digest:None,relations:vec![]});}
   Ok(captures)
  }.boxed()
    }
}
fn fixture_pdf(text: &str) -> Vec<u8> {
    let stream = format!("BT /F1 12 Tf 40 100 Td ({text}) Tj ET\n");
    let objects=["<< /Type /Catalog /Pages 2 0 R >>".into(),"<< /Type /Pages /Kids [3 0 R] /Count 1 >>".into(),"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>".into(),"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>".into(),format!("<< /Length {} >>\nstream\n{stream}endstream",stream.len())];
    let mut out = String::from("%PDF-1.4\n");
    let mut offsets = vec![];
    for (i, o) in objects.iter().enumerate() {
        offsets.push(out.len());
        out.push_str(&format!("{} 0 obj\n{o}\nendobj\n", i + 1));
    }
    let xref = out.len();
    out.push_str("xref\n0 6\n0000000000 65535 f \n");
    for off in offsets {
        out.push_str(&format!("{off:010} 00000 n \n"));
    }
    out.push_str(&format!(
        "trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n"
    ));
    out.into_bytes()
}

#[tokio::test]
async fn da04_capture_only_hydrates_shared_sources_and_retains_bounded_context_and_documents()
-> anyhow::Result<()> {
    let root = private_temp()?;
    let store = Store::open(root.path().join("state"))?;
    let mut config = Config {
        state_dir: store.root().into(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    config.limits.graph_nodes = 16;
    let original = store.record_activity(&discovery("101", "10", "day1"))?;
    let mut repost = discovery("303", "10", "day2");
    repost.kind = ActivityKind::Repost;
    store.record_activity(&repost)?;
    let mut continuation = discovery("101", "11", "day1");
    continuation.kind = ActivityKind::Reply;
    continuation.capture.relations.push(Related {
        target: Identity::x("10")?,
        kind: RelationKind::RepliesTo,
    });
    store.record_activity(&continuation)?;
    let collector = FixtureCollector::new();
    let cancel = tokio_util::sync::CancellationToken::new();
    for _ in 0..12 {
        telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
            .await?;
    }
    let revision = store
        .current_revision(&Identity::x("10")?)?
        .expect("autonomous capture must retain source text");
    assert!(
        store
            .revision(&revision)?
            .content
            .body
            .contains("Original complete root")
    );
    for id in ["10", "11", "20", "30", "40"] {
        assert_eq!(
            collector.count(id),
            1,
            "shared/restarted context must be reused: {id}"
        );
    }
    assert_eq!(collector.count("https://example.org/article"), 1);
    assert_eq!(collector.count("https://example.org/paper.pdf"), 1);
    assert!(
        store
            .revision(&store.current_revision(&Identity::x("20")?)?.unwrap())?
            .content
            .body
            .contains("Full retained Article body")
    );
    assert!(
        store
            .revision(&store.current_revision(&Identity::x("40")?)?.unwrap())?
            .content
            .body
            .contains("Useful embedded")
    );
    assert!(
        store
            .revision(
                &store
                    .current_revision(&Identity::from_url("https://example.org/paper.pdf")?)?
                    .unwrap()
            )?
            .content
            .body
            .contains("Retained PDF source text")
    );
    assert_eq!(
        store.activity_observation(&original.id)?,
        original,
        "timeline observation pins remain immutable"
    );
    let hydrated = store.activity_observations(&original.activity.id()?)?;
    assert_eq!(hydrated.len(), 2);
    let latest = hydrated
        .iter()
        .find(|o| o.provenance.get("da04_hydration").is_some())
        .unwrap();
    let view = store.view(&latest.view_id)?;
    assert!(
        view.members
            .iter()
            .any(|m| m.source_id == Identity::x("11").unwrap().id()),
        "known same-author continuation is context"
    );
    assert!(
        view.retrieval.as_ref().unwrap()["contexts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["time_relation"] == "outside_window")
    );
    assert!(
        view.omissions
            .iter()
            .any(|o| o.reason.contains("missing") || o.reason.contains("nontext"))
    );
    let conn = rusqlite::Connection::open(store.root().join("archive.sqlite"))?;
    for table in [
        "jobs",
        "outbox",
        "delivery_attempts",
        "transformation_runs",
        "polling_cursors",
    ] {
        assert_eq!(
            conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))?,
            0,
            "capture only must not invoke {table}"
        );
    }
    assert!(store.integrity()? > 0);
    config.archive.directory = Some(root.path().join("archive"));
    config.archive.branch = Some("main".into());
    config.tools.git = "/usr/bin/git".into();
    store.project(&config, 100, &cancel).await?;
    let exported: serde_json::Value = serde_json::from_slice(&std::fs::read(
        config
            .archive
            .directory
            .as_ref()
            .unwrap()
            .join(format!("archive-v1/views/{}.json", latest.view_id)),
    )?)?;
    assert!(!exported["omissions"].as_array().unwrap().is_empty());
    assert!(
        !exported["retrieval"]["attempt_history"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    Ok(())
}

#[test]
fn da04_article_entity_destinations_are_capture_links() -> anyhow::Result<()> {
    let post = telegram_archive_bot::x::parse_fx(
        "20",
        &json!({"status":{"id":"20","text":"Article","article":{"content":{"blocks":[{"type":"unstyled","text":"Full body"}],"entityMap":[{"key":"1","value":{"type":"LINK","data":{"url":"https://example.org/reference.pdf#page=2"}}}]}}}}),
    )?;
    assert!(
        post.relations
            .iter()
            .any(|r| r.target.url() == "https://example.org/reference.pdf")
    );
    Ok(())
}

#[tokio::test]
async fn da04_autonomous_loop_exposes_limits_without_any_derivative_or_delivery()
-> anyhow::Result<()> {
    use std::{sync::Arc, time::Duration};
    use telegram_archive_bot::config::ConfigManager;
    let root = private_temp()?;
    let store = Store::open(root.path())?;
    let origin = store.record_activity(&discovery("101", "10", "bounded"))?;
    let mut config = Config {
        state_dir: root.path().into(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    config.daily.capture.max_links = 0;
    config.limits.graph_depth = 1;
    config.limits.reply_ancestors = 0;
    let cancel = tokio_util::sync::CancellationToken::new();
    let configs = Arc::new(tokio::sync::RwLock::new(ConfigManager::new(config)?));
    let collector = Arc::new(FixtureCollector::new());
    let worker = tokio::spawn(telegram_archive_bot::daily_capture::run_with(
        store.clone(),
        configs,
        collector.clone(),
        cancel.clone(),
    ));
    let _cancel_on_drop = cancel.clone().drop_guard();
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store
                .activity_observations(&origin.activity.id()?)
                .is_ok_and(|v| v.len() > 1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), worker).await???;
    ready??;
    let observations = store.activity_observations(&origin.activity.id()?)?;
    let hydrated = observations
        .iter()
        .find(|o| o.provenance.get("da04_hydration").is_some())
        .unwrap();
    let view = store.view(&hydrated.view_id)?;
    for reason in ["external_link_limit", "reply_ancestor_limit", "depth_limit"] {
        assert!(
            view.omissions.iter().any(|o| o.reason == reason),
            "missing explicit {reason}"
        );
    }
    assert_eq!(collector.count("https://example.org/article"), 0);
    assert_eq!(collector.count("https://example.org/paper.pdf"), 0);
    let conn = rusqlite::Connection::open(root.path().join("archive.sqlite"))?;
    for table in [
        "jobs",
        "outbox",
        "delivery_attempts",
        "transformation_runs",
        "polling_cursors",
        "projection_pushes",
    ] {
        assert_eq!(
            conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                .get::<_, i64>(0))?,
            0
        );
    }
    Ok(())
}

struct TimeoutCollector {
    inner: FixtureCollector,
    started: std::sync::atomic::AtomicBool,
    once: std::sync::atomic::AtomicBool,
}
impl telegram_archive_bot::daily_capture::Collector for TimeoutCollector {
    fn collect<'a>(
        &'a self,
        s: &'a Store,
        c: &'a Config,
        i: &'a Identity,
        cancel: &'a tokio_util::sync::CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<Vec<Capture>>> {
        use futures_util::FutureExt;
        async move{if i.external_id=="40"&&self.once.swap(false,std::sync::atomic::Ordering::SeqCst){self.started.store(true,std::sync::atomic::Ordering::SeqCst);tokio::select!{_=cancel.cancelled()=>anyhow::bail!("fixture cancelled"),_=tokio::time::sleep(std::time::Duration::from_secs(10))=>{}}}self.inner.collect(s,c,i,cancel).await}.boxed()
    }
}
#[tokio::test]
async fn da04_timeout_restore_hold_and_restart_reuse_completed_document_nodes() -> anyhow::Result<()>
{
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    let root = private_temp()?;
    let store = Store::open(root.path().join("state"))?;
    store.record_activity(&discovery("101", "10", "restart"))?;
    let mut config = Config {
        state_dir: store.root().into(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    config.daily.capture.retry_seconds = 1;
    config.limits.download_seconds = 2;
    let collector = Arc::new(TimeoutCollector {
        inner: FixtureCollector::new(),
        started: AtomicBool::new(false),
        once: AtomicBool::new(true),
    });
    let cancel = tokio_util::sync::CancellationToken::new();
    let s = store.clone();
    let c = config.clone();
    let f = collector.clone();
    let token = cancel.clone();
    let worker = tokio::spawn(async move {
        telegram_archive_bot::daily_capture::step_with(&s, &c, f.as_ref(), &token).await
    });
    let started = tokio::time::timeout(Duration::from_secs(3), async {
        while !collector.started.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    if let Err(error) = started {
        cancel.cancel();
        worker.await??;
        return Err(error.into());
    }
    let backup = root.path().join("backup");
    store.backup(&backup)?;
    Store::verify_backup(&backup)?;
    let restored_path = root.path().join("restored");
    Store::restore_backup(&backup, &restored_path)?;
    let restored = Store::open(&restored_path)?;
    let held = telegram_archive_bot::daily_capture::step_with(
        &restored,
        &config,
        collector.as_ref(),
        &cancel,
    )
    .await?;
    assert!(held.paused);
    worker.await??;
    assert_eq!(collector.inner.count("https://example.org/article"), 1);
    assert_eq!(collector.inner.count("https://example.org/paper.pdf"), 1);
    let db = rusqlite::Connection::open(restored_path.join("archive.sqlite"))?;
    assert_eq!(
        db.query_row("SELECT state FROM daily_capture_work", [], |r| r
            .get::<_, String>(0))?,
        "retry"
    );
    restored.release_recovery_hold()?;
    for _ in 0..5 {
        telegram_archive_bot::daily_capture::step_with(
            &restored,
            &config,
            collector.as_ref(),
            &cancel,
        )
        .await?;
    }
    assert_eq!(collector.inner.count("10"), 1);
    assert_eq!(collector.inner.count("https://example.org/article"), 1);
    assert_eq!(collector.inner.count("https://example.org/paper.pdf"), 1);
    assert_eq!(
        db.query_row("SELECT state FROM daily_capture_work", [], |r| r
            .get::<_, String>(0))?,
        "complete"
    );
    let attempts = db
        .prepare("SELECT state FROM daily_capture_attempts ORDER BY attempt")?
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    assert_eq!(
        attempts,
        ["interrupted", "complete"],
        "restart must retain the interrupted attempt receipt"
    );
    assert!(restored.integrity()? > 0);
    Ok(())
}

struct EditableCollector {
    version: std::sync::atomic::AtomicUsize,
}
impl telegram_archive_bot::daily_capture::Collector for EditableCollector {
    fn collect<'a>(
        &'a self,
        _s: &'a Store,
        _c: &'a Config,
        i: &'a Identity,
        _cancel: &'a tokio_util::sync::CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<Vec<Capture>>> {
        use futures_util::FutureExt;
        async move {
            Ok(vec![Capture {
                identity: i.clone(),
                submitted_url: None,
                content: Some(Content {
                    body: format!(
                        "Original version {}",
                        self.version.load(std::sync::atomic::Ordering::SeqCst)
                    ),
                    metadata: Default::default(),
                }),
                status: FetchStatus::Complete,
                extractor: "editable-fixture".into(),
                evidence: json!({"fixture":true}),
                raw_digest: None,
                relations: vec![],
            }])
        }
        .boxed()
    }
}
#[tokio::test]
async fn da04_refresh_preserves_prior_revision_and_every_activity_pin() -> anyhow::Result<()> {
    let root = private_temp()?;
    let store = Store::open(root.path())?;
    let origin = store.record_activity(&discovery("101", "10", "edits"))?;
    let mut config = Config {
        state_dir: root.path().into(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    config.daily.capture.refresh_seconds = 86400;
    let collector = EditableCollector {
        version: std::sync::atomic::AtomicUsize::new(1),
    };
    let cancel = tokio_util::sync::CancellationToken::new();
    for _ in 0..3 {
        telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
            .await?;
    }
    let first = store.current_revision(&origin.activity.source)?.unwrap();
    collector
        .version
        .store(2, std::sync::atomic::Ordering::SeqCst);
    let db = rusqlite::Connection::open(root.path().join("archive.sqlite"))?;
    db.execute_batch("UPDATE daily_capture_cache SET observed_at=observed_at-86401; UPDATE daily_capture_work SET next_at=0;")?;
    for _ in 0..4 {
        telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
            .await?;
    }
    let second = store.current_revision(&origin.activity.source)?.unwrap();
    assert_ne!(first, second);
    assert_eq!(store.revision(&first)?.content.body, "Original version 1");
    assert_eq!(store.revision(&second)?.content.body, "Original version 2");
    assert_eq!(store.activity_observation(&origin.id)?, origin);
    let observations = store.activity_observations(&origin.activity.id()?)?;
    assert_eq!(observations.len(), 3);
    assert!(
        observations
            .iter()
            .any(|o| o.revision_id.as_deref() == Some(&first))
    );
    assert!(
        observations
            .iter()
            .any(|o| o.revision_id.as_deref() == Some(&second))
    );
    Ok(())
}

#[tokio::test]
async fn da04_global_lease_prevents_concurrent_duplicate_capture_and_stale_publish()
-> anyhow::Result<()> {
    use std::sync::Arc;
    struct Gate {
        started: tokio::sync::Notify,
        release: tokio::sync::Notify,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl telegram_archive_bot::daily_capture::Collector for Gate {
        fn collect<'a>(
            &'a self,
            _s: &'a Store,
            _c: &'a Config,
            i: &'a Identity,
            _cancel: &'a tokio_util::sync::CancellationToken,
        ) -> futures_util::future::BoxFuture<'a, anyhow::Result<Vec<Capture>>> {
            use futures_util::FutureExt;
            async move {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.started.notify_one();
                self.release.notified().await;
                Ok(vec![Capture {
                    identity: i.clone(),
                    submitted_url: None,
                    content: Some(Content {
                        body: "Owned original".into(),
                        metadata: Default::default(),
                    }),
                    status: FetchStatus::Complete,
                    extractor: "gated-fixture".into(),
                    evidence: json!({}),
                    raw_digest: None,
                    relations: vec![],
                }])
            }
            .boxed()
        }
    }
    let root = private_temp()?;
    let store = Store::open(root.path())?;
    store.record_activity(&discovery("101", "10", "concurrent"))?;
    let mut config = Config {
        state_dir: root.path().into(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    config.limits.max_attempts = 1;
    let collector = Arc::new(Gate {
        started: Default::default(),
        release: Default::default(),
        calls: Default::default(),
    });
    let cancel = tokio_util::sync::CancellationToken::new();
    let s = store.clone();
    let c = config.clone();
    let f = collector.clone();
    let token = cancel.clone();
    let worker = tokio::spawn(async move {
        telegram_archive_bot::daily_capture::step_with(&s, &c, f.as_ref(), &token).await
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        collector.started.notified(),
    )
    .await?;
    assert!(
        !telegram_archive_bot::daily_capture::step_with(
            &store,
            &config,
            collector.as_ref(),
            &cancel
        )
        .await?
        .captured
    );
    let db = rusqlite::Connection::open(root.path().join("archive.sqlite"))?;
    db.execute("UPDATE daily_capture_lease SET generation=generation+1", [])?;
    collector.release.notify_one();
    assert!(worker.await?.is_err());
    assert!(
        store.current_revision(&Identity::x("10")?)?.is_none(),
        "stale fetch cannot publish canonical source text"
    );
    assert_eq!(collector.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    db.execute("UPDATE daily_capture_lease SET lease_until=0", [])?;
    let healthy = EditableCollector {
        version: std::sync::atomic::AtomicUsize::new(2),
    };
    telegram_archive_bot::daily_capture::step_with(&store, &config, &healthy, &cancel).await?;
    assert!(
        store.current_revision(&Identity::x("10")?)?.is_none(),
        "exhausted interrupted attempt cannot make another fetch/publish"
    );
    assert_eq!(store.activity_capture_status()?["sources"]["failed"], 1);
    Ok(())
}

#[tokio::test]
async fn da04_public_capture_status_and_scoped_retry_do_not_send_or_echo_urls() -> anyhow::Result<()>
{
    struct Failing;
    impl telegram_archive_bot::daily_capture::Collector for Failing {
        fn collect<'a>(
            &'a self,
            _s: &'a Store,
            _c: &'a Config,
            _i: &'a Identity,
            _t: &'a tokio_util::sync::CancellationToken,
        ) -> futures_util::future::BoxFuture<'a, anyhow::Result<Vec<Capture>>> {
            use futures_util::FutureExt;
            async { anyhow::bail!("fixture network failure") }.boxed()
        }
    }
    let root = private_temp()?;
    let store = Store::open(root.path()).context("retry fixture Store::open")?;
    store
        .record_activity(&discovery("101", "10", "retry"))
        .context("retry fixture discovery")?;
    let mut config = Config {
        state_dir: root.path().into(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    config.limits.max_attempts = 1;
    let path = root.path().join("config.toml");
    std::fs::write(&path, toml::to_string(&config)?)?;
    let cancel = tokio_util::sync::CancellationToken::new();
    telegram_archive_bot::daily_capture::step_with(&store, &config, &Failing, &cancel)
        .await
        .context("initial failed capture and receipt")?;
    let cli = |args: &[&str]| -> anyhow::Result<serde_json::Value> {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_telegram-archive-bot"))
            .arg("--config")
            .arg(&path)
            .args(args)
            .output()?;
        anyhow::ensure!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!String::from_utf8_lossy(&out.stdout).contains("https://"));
        Ok(serde_json::from_slice(&out.stdout)?)
    };
    assert_eq!(
        cli(&["daily-capture-status"]).context("CLI capture status")?["sources"]["failed"],
        1
    );
    let id = Identity::x("10")?.id();
    cli(&["daily-capture-retry", &id]).context("CLI capture retry")?;
    let collector = EditableCollector {
        version: std::sync::atomic::AtomicUsize::new(2),
    };
    for index in 0..4 {
        telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
            .await
            .with_context(|| format!("healthy retry step {index}"))?;
    }
    assert_eq!(
        cli(&["daily-capture-status"]).context("final CLI capture status")?["sources"]["complete"],
        1
    );
    Ok(())
}

#[tokio::test]
async fn da04_graph_byte_budget_marks_oversized_nodes_without_claiming_text() -> anyhow::Result<()>
{
    struct Oversized;
    impl telegram_archive_bot::daily_capture::Collector for Oversized {
        fn collect<'a>(
            &'a self,
            _s: &'a Store,
            _c: &'a Config,
            i: &'a Identity,
            _t: &'a tokio_util::sync::CancellationToken,
        ) -> futures_util::future::BoxFuture<'a, anyhow::Result<Vec<Capture>>> {
            use futures_util::FutureExt;
            async move {
                Ok(vec![Capture {
                    identity: i.clone(),
                    submitted_url: None,
                    content: Some(Content {
                        body: "x".repeat(1025),
                        metadata: Default::default(),
                    }),
                    status: FetchStatus::Complete,
                    extractor: "oversized-fixture".into(),
                    evidence: json!({}),
                    raw_digest: None,
                    relations: vec![],
                }])
            }
            .boxed()
        }
    }
    let root = private_temp()?;
    let store = Store::open(root.path())?;
    let origin = store.record_activity(&discovery("101", "10", "bytes"))?;
    let mut config = Config {
        state_dir: root.path().into(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    config.limits.input_bytes = 1024;
    let cancel = tokio_util::sync::CancellationToken::new();
    for _ in 0..3 {
        telegram_archive_bot::daily_capture::step_with(&store, &config, &Oversized, &cancel)
            .await?;
    }
    assert!(
        store.current_revision(&origin.activity.source)?.is_none(),
        "over-budget text is not accepted as captured original"
    );
    let observations = store.activity_observations(&origin.activity.id()?)?;
    let hydrated = observations
        .iter()
        .find(|v| v.provenance.get("da04_hydration").is_some())
        .unwrap();
    let view = store.view(&hydrated.view_id)?;
    assert!(view.omissions.iter().any(|o| o.reason == "byte_limit"));
    assert!(view.retrieval.unwrap()["bytes"].as_u64().unwrap() <= 1024);
    Ok(())
}

#[test]
fn da04_schema14_upgrade_preserves_activity_pins_and_admits_them() -> anyhow::Result<()> {
    let root = private_temp()?;
    let store = Store::open(root.path())?;
    let origin = store.record_activity(&discovery("101", "10", "migration"))?;
    drop(store);
    let historical = private_temp()?;
    let database = historical.path().join("archive.sqlite");
    let db = rusqlite::Connection::open(&database)?;
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&database, std::fs::Permissions::from_mode(0o600))?;
    }
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
        include_str!("../migrations/012_observation_source.sql"),
        include_str!("../migrations/013_account_activities.sql"),
        include_str!("../migrations/014_daily_planner.sql"),
    ] {
        db.execute_batch(migration)?;
    }
    db.execute(
        "ATTACH DATABASE ?1 AS captured",
        [root
            .path()
            .join("archive.sqlite")
            .to_string_lossy()
            .as_ref()],
    )?;
    db.execute_batch("BEGIN; PRAGMA defer_foreign_keys=ON;")?;
    for table in [
        "sources",
        "source_revisions",
        "observations",
        "views",
        "source_aliases",
        "relations",
        "accounts",
        "account_observations",
        "discovery_runs",
        "activities",
        "activity_observations",
    ] {
        db.execute_batch(&format!(
            "INSERT INTO {table} SELECT * FROM captured.{table}"
        ))?;
    }
    db.execute_batch("COMMIT; DETACH DATABASE captured; PRAGMA user_version=14;")?;
    drop(db);
    let root = historical;
    let store = Store::open(root.path())?;
    assert_eq!(store.activity_observation(&origin.id)?, origin);
    let mut config = Config {
        state_dir: root.path().into(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    assert_eq!(store.admit_activity_captures(&config, 100)?, 1);
    assert_eq!(store.integrity()?, 0);
    Ok(())
}

struct FixedCapture(Capture);
impl telegram_archive_bot::daily_capture::Collector for FixedCapture {
    fn collect<'a>(
        &'a self,
        _: &'a Store,
        _: &'a Config,
        _: &'a Identity,
        _: &'a tokio_util::sync::CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<Vec<Capture>>> {
        use futures_util::FutureExt;
        async move { Ok(vec![self.0.clone()]) }.boxed()
    }
}
fn body_capture(body: Option<&str>, status: FetchStatus) -> Capture {
    Capture {
        identity: Identity::x("10").unwrap(),
        submitted_url: None,
        content: body.map(|s| Content {
            body: s.into(),
            metadata: ContentMetadata::default(),
        }),
        status,
        extractor: "fixed-fixture".into(),
        evidence: json!({}),
        raw_digest: None,
        relations: vec![],
    }
}
#[tokio::test]
async fn hydration_materialization_never_recaptures_or_rolls_back_newer_revision()
-> anyhow::Result<()> {
    let root = private_temp()?;
    let state = root.path().join("state");
    let store = Store::open(&state)?;
    let mut config = Config {
        state_dir: state.clone(),
        ..Config::default()
    };
    config.daily.capture.enabled = true;
    store.record_activity(&discovery("101", "10", "day1"))?;
    let collector = FixedCapture(body_capture(Some("version one"), FetchStatus::Complete));
    let cancel = tokio_util::sync::CancellationToken::new();
    assert!(
        telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
            .await?
            .captured
    );
    store.capture(&body_capture(
        Some("newer version two"),
        FetchStatus::Complete,
    ))?;
    let newest = store.current_revision(&Identity::x("10")?)?;
    drop(store);
    let connection = rusqlite::Connection::open(state.join("archive.sqlite"))?;
    let before: i64 =
        connection.query_row("SELECT count(*) FROM observations", [], |r| r.get(0))?;
    drop(connection);
    let store = Store::open(&state)?;
    assert_eq!(
        telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
            .await?
            .materialized,
        1
    );
    assert_eq!(
        store.current_revision(&Identity::x("10")?)?,
        newest,
        "historical materialization must preserve newer canonical body"
    );
    drop(store);
    let connection = rusqlite::Connection::open(state.join("archive.sqlite"))?;
    assert_eq!(
        connection.query_row("SELECT count(*) FROM observations", [], |r| r
            .get::<_, i64>(0))?,
        before,
        "materialization references immutable observations without recapture"
    );
    connection.execute("UPDATE daily_capture_cache SET observation_ids=(SELECT json_array(max(id)) FROM observations WHERE source_id=daily_capture_cache.source_id)",[])?;
    connection.execute(
        "UPDATE daily_capture_work SET render_pending=1,render_cursor=0",
        [],
    )?;
    drop(connection);
    let store = Store::open(&state)?;
    assert_eq!(
        telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
            .await?
            .materialized,
        1
    );
    assert_eq!(store.current_revision(&Identity::x("10")?)?, newest);
    Ok(())
}
#[tokio::test]
async fn hydration_byte_budget_counts_retained_revision_after_partial_or_missing_fetch()
-> anyhow::Result<()> {
    for (text, status) in [
        (Some("tiny preview"), FetchStatus::Partial),
        (None, FetchStatus::Missing),
    ] {
        let root = private_temp()?;
        let state = root.path().join("state");
        let store = Store::open(&state)?;
        let mut config = Config {
            state_dir: state.clone(),
            ..Config::default()
        };
        config.daily.capture.enabled = true;
        config.limits.input_bytes = 100;
        store.capture(&body_capture(
            Some(&"L".repeat(4096)),
            FetchStatus::Complete,
        ))?;
        let retained = store.current_revision(&Identity::x("10")?)?;
        store.record_activity(&discovery("101", "10", "day1"))?;
        let collector = FixedCapture(body_capture(text, status));
        let cancel = tokio_util::sync::CancellationToken::new();
        assert!(
            telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
                .await?
                .captured
        );
        assert_eq!(store.current_revision(&Identity::x("10")?)?, retained);
        drop(store);
        let connection = rusqlite::Connection::open(state.join("archive.sqlite"))?;
        let view_id: String = connection.query_row(
            "SELECT json_extract(result,'$.view_id') FROM daily_capture_work",
            [],
            |r| r.get(0),
        )?;
        drop(connection);
        let store = Store::open(&state)?;
        let view = store.view(&view_id)?;
        assert!(
            view.members[0].revision_id.is_none(),
            "oversized prior complete body must not enter bounded view"
        );
        assert!(view.omissions.iter().any(|o| o.reason == "byte_limit"));
        assert_eq!(view.retrieval.unwrap()["bytes"], 0);
    }
    Ok(())
}

#[tokio::test]
async fn retry_with_changed_limits_rebuilds_bounded_view_from_cached_nodes() -> anyhow::Result<()> {
    use futures_util::FutureExt;
    struct RetryGraph(std::sync::Mutex<std::collections::BTreeMap<String, usize>>);
    impl telegram_archive_bot::daily_capture::Collector for RetryGraph {
        fn collect<'a>(
            &'a self,
            _: &'a Store,
            _: &'a Config,
            identity: &'a Identity,
            _: &'a tokio_util::sync::CancellationToken,
        ) -> futures_util::future::BoxFuture<'a, anyhow::Result<Vec<Capture>>> {
            async move {
                *self
                    .0
                    .lock()
                    .unwrap()
                    .entry(identity.external_id.clone())
                    .or_default() += 1;
                anyhow::ensure!(
                    identity.external_id != "30",
                    "synthetic interrupted context"
                );
                let mut capture = body_capture(
                    Some(&if identity.external_id == "10" {
                        "L".repeat(4096)
                    } else {
                        "context".into()
                    }),
                    FetchStatus::Complete,
                );
                capture.identity = identity.clone();
                capture.relations = vec![Related {
                    target: Identity::x(if identity.external_id == "10" {
                        "20"
                    } else {
                        "30"
                    })?,
                    kind: RelationKind::Quotes,
                }];
                Ok(vec![capture])
            }
            .boxed()
        }
    }
    for limit in ["bytes", "nodes"] {
        let root = private_temp()?;
        let state = root.path().join("state");
        let store = Store::open(&state)?;
        let mut config = Config {
            state_dir: state.clone(),
            ..Config::default()
        };
        config.daily.capture.enabled = true;
        config.limits.max_attempts = 1;
        config.limits.input_bytes = 8192;
        config.limits.graph_nodes = 16;
        store.record_activity(&discovery("101", "10", limit))?;
        let collector = RetryGraph(Default::default());
        let cancel = tokio_util::sync::CancellationToken::new();
        telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
            .await?;
        assert_eq!(store.activity_capture_status()?["sources"]["failed"], 1);
        assert_eq!(collector.0.lock().unwrap().get("10"), Some(&1));
        if limit == "bytes" {
            config.limits.input_bytes = 100;
        } else {
            config.limits.graph_nodes = 1;
        }
        store.retry_activity_capture(&config, &Identity::x("10")?.id())?;
        telegram_archive_bot::daily_capture::step_with(&store, &config, &collector, &cancel)
            .await?;
        assert_eq!(store.activity_capture_status()?["sources"]["complete"], 1);
        assert_eq!(
            collector.0.lock().unwrap().get("10"),
            Some(&1),
            "retained root must come from canonical cache"
        );
        drop(store);
        let db = rusqlite::Connection::open(state.join("archive.sqlite"))?;
        let id: String = db.query_row(
            "SELECT json_extract(result,'$.view_id') FROM daily_capture_work",
            [],
            |r| r.get(0),
        )?;
        drop(db);
        let store = Store::open(&state)?;
        let view = store.view(&id)?;
        if limit == "bytes" {
            assert!(
                view.members.iter().all(|m| m.revision_id.is_none()),
                "changed byte bound must not inherit oversized checkpoint text"
            );
            assert_eq!(view.retrieval.as_ref().unwrap()["bytes"], 0);
        } else {
            assert!(
                view.members.len() <= 1,
                "changed node bound must not inherit extra checkpoint members"
            );
            assert!(view.members[0].revision_id.is_some());
        }
        assert!(!view.omissions.is_empty());
        assert!(
            store.current_revision(&Identity::x("10")?)?.is_some(),
            "canonical text is retained independently of traversal bounds"
        );
    }
    Ok(())
}
