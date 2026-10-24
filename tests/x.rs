use anyhow::Result;
use futures_util::{FutureExt, future::BoxFuture};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs::Permissions,
    os::unix::fs::PermissionsExt,
    sync::{Arc, Mutex},
    time::Duration,
};
use telegram_archive_bot::{
    config::Config,
    jobs::Failure,
    sources::{Content, ContentMetadata, FetchStatus, Identity, Related, RelationKind},
    storage::Store,
    x::{Post, Provider, XGraph, parse_fx, parse_official},
};
use tokio_util::sync::CancellationToken;

struct Fake {
    name: &'static str,
    posts: BTreeMap<String, Post>,
    calls: Mutex<BTreeMap<String, u32>>,
}
impl Provider for Fake {
    fn name(&self) -> &'static str {
        self.name
    }
    fn article_only(&self) -> bool {
        self.name == "guest-only"
    }
    fn fetch<'a>(
        &'a self,
        id: &'a str,
        _max: u64,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<Post, Failure>> {
        async move {
            *self.calls.lock().unwrap().entry(id.into()).or_default()+=1;
            tokio::select! {_=cancel.cancelled()=>return Err(Failure::Cancelled),_=tokio::time::sleep(Duration::from_millis(10))=>{}}
            self.posts.get(id).cloned().ok_or(Failure::Unavailable)
        }.boxed()
    }
}

#[tokio::test]
async fn guest_enrichment_runs_only_for_evidenced_missing_article_body() -> Result<()> {
    for (evidence, needed) in [
        (json!({"limitation":"ordinary_partial_post"}), false),
        (
            json!({"article_hydrated":true,"limitations":["article_nontext_entity_retained_only_in_raw"]}),
            false,
        ),
        (
            json!({"article_hydrated":false,"limitations":["article_preview_only"]}),
            true,
        ),
        (
            json!({"limitation":"article_link_requires_fallback_hydration"}),
            true,
        ),
    ] {
        let (_dir, store) = store()?;
        let mut preview = post("100", "Retained preview", vec![]);
        preview.status = FetchStatus::Partial;
        preview.evidence = evidence;
        let primary = fake(vec![preview]);
        let mut guest = fake(vec![post("100", "Hydrated article", vec![])]);
        Arc::get_mut(&mut guest).unwrap().name = "guest-only";
        let graph = XGraph::new(vec![primary, guest.clone()]);
        let traversal = graph
            .traverse(
                &store,
                "https://x.com/i/status/100",
                &Config::default(),
                false,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            guest.calls.lock().unwrap().get("100").copied().unwrap_or(0),
            u32::from(needed)
        );
        assert_eq!(traversal.captures.len(), if needed { 2 } else { 1 });
        traversal.persist(&store)?;
        store.integrity()?;
    }
    Ok(())
}
fn post(id: &str, body: &str, relations: Vec<(&str, RelationKind)>) -> Post {
    Post {
        id: id.into(),
        content: Some(Content {
            body: body.into(),
            metadata: ContentMetadata::default(),
        }),
        status: FetchStatus::Complete,
        relations: relations
            .into_iter()
            .map(|(id, kind)| Related {
                target: Identity::x(id).unwrap(),
                kind,
            })
            .collect(),
        previews: vec![],
        evidence: json!({"synthetic":true}),
        raw: format!("{{\"id\":\"{id}\"}}").into_bytes(),
    }
}
fn fake(posts: Vec<Post>) -> Arc<Fake> {
    Arc::new(Fake {
        name: "synthetic-v1",
        posts: posts.into_iter().map(|p| (p.id.clone(), p)).collect(),
        calls: Mutex::new(BTreeMap::new()),
    })
}
fn store() -> Result<(tempfile::TempDir, Store)> {
    let dir = tempfile::Builder::new()
        .permissions(Permissions::from_mode(0o700))
        .tempdir_in(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))?;
    let store = Store::open(dir.path())?;
    Ok((dir, store))
}

#[tokio::test]
async fn shared_graph_cycle_and_concurrent_aliases_fetch_each_identity_once() -> Result<()> {
    let (_dir, store) = store()?;
    let provider = fake(vec![
        post("100", "A", vec![("200", RelationKind::Quotes)]),
        post(
            "200",
            "B",
            vec![
                ("100", RelationKind::Quotes),
                ("300", RelationKind::RepliesTo),
            ],
        ),
        post("300", "C", vec![("100", RelationKind::RepliesTo)]),
    ]);
    let graph = XGraph::new(vec![provider.clone()]);
    let config = Config::default();
    let cancel = CancellationToken::new();
    let (b, c) = tokio::join!(
        graph.traverse(
            &store,
            "https://twitter.com/old/status/200",
            &config,
            false,
            &cancel
        ),
        graph.traverse(
            &store,
            "https://x.com/new/status/300",
            &config,
            false,
            &cancel
        )
    );
    let b = b?;
    let c = c?;
    assert_eq!(b.visited.len(), 3);
    assert_eq!(c.visited.len(), 3);
    let view = b.persist(&store)?;
    c.persist(&store)?;
    assert_eq!(store.source_counts()?.0, 3);
    assert_eq!(store.source_counts()?.1, 3);
    assert!(
        provider
            .calls
            .lock()
            .unwrap()
            .values()
            .all(|calls| *calls == 1)
    );
    assert_eq!(store.view(&view.id()?)?, view);
    assert_eq!(view.members.len(), 3);
    assert!(view.retrieval.is_some());
    Ok(())
}

#[test]
fn provider_parsers_hydrate_articles_preserve_references_and_classify_tombstones() -> Result<()> {
    let official = parse_official(
        "100",
        &json!({"data":{"id":"100","text":"preview","note_tweet":{"text":"完整 Unicode long form"},"referenced_tweets":[{"type":"retweeted","id":"200"}]}}),
    )?;
    assert_eq!(official.content.unwrap().body, "完整 Unicode long form");
    assert_eq!(official.relations[0].kind, RelationKind::Retweets);
    let article = parse_fx(
        "100",
        &json!({"code":200,"status":{"id":"100","text":"Article introduction","article":{"title":"Synthetic article","content":{"blocks":[{"type":"header-one","text":"Heading"},{"type":"unstyled","text":"Full article body 中文"},{"type":"code-block","text":"let x = 1;"}]}},"quote":{"id":"200","text":"quote excerpt"},"replying_to":{"status":"300"}}}),
    )?;
    assert_eq!(article.status, FetchStatus::Complete);
    assert!(
        article
            .content
            .unwrap()
            .body
            .contains("Full article body 中文")
    );
    assert_eq!(article.previews[0].0, "200");
    assert_eq!(article.relations.len(), 2);
    let preview = parse_fx(
        "100",
        &json!({"code":200,"status":{"id":"100","text":"Article","article":{"preview_text":"only an excerpt"}}}),
    )?;
    assert_eq!(preview.status, FetchStatus::Partial);
    for (reason, status) in [
        ("deleted", FetchStatus::Deleted),
        ("private", FetchStatus::Forbidden),
        ("unavailable", FetchStatus::Missing),
    ] {
        assert_eq!(
            parse_fx(
                "100",
                &json!({"status":{"type":"tombstone","reason":reason}})
            )?
            .status,
            status
        );
    }
    assert_eq!(
        parse_fx("100", &json!({"code":401}))?.status,
        FetchStatus::Forbidden
    );
    assert!(parse_official("100", &json!({"data":{"id":"999","text":"wrong object"}})).is_err());
    Ok(())
}

#[tokio::test]
async fn fallback_provenance_and_preview_never_downgrade_a_full_capture() -> Result<()> {
    let (_dir, store) = store()?;
    let config = Config::default();
    let cancel = CancellationToken::new();
    let original = XGraph::new(vec![fake(vec![post("100", "Full A original", vec![])])]);
    original
        .traverse(
            &store,
            "https://x.com/a/status/100",
            &config,
            false,
            &cancel,
        )
        .await?
        .persist(&store)?;
    let a1 = store.current_revision(&Identity::x("100")?)?.unwrap();
    let mut denied = post("200", "discarded", vec![]);
    denied.content = None;
    denied.status = FetchStatus::Forbidden;
    let mut b = post("200", "B quotes A", vec![("100", RelationKind::Quotes)]);
    b.previews.push((
        "100".into(),
        Content {
            body: "short A excerpt".into(),
            metadata: ContentMetadata::default(),
        },
    ));
    let mut missing = post("100", "discarded", vec![]);
    missing.content = None;
    missing.status = FetchStatus::Missing;
    let first = Arc::new(Fake {
        name: "auth-provider",
        posts: BTreeMap::from([("200".into(), denied)]),
        calls: Mutex::new(BTreeMap::new()),
    });
    let graph = XGraph::new(vec![first, fake(vec![b, missing])]);
    let result = graph
        .traverse(
            &store,
            "https://x.com/b/status/200",
            &config,
            false,
            &cancel,
        )
        .await?;
    assert_eq!(result.captures[0].extractor, "auth-provider");
    assert_eq!(result.captures[0].status, FetchStatus::Forbidden);
    assert!(
        result
            .captures
            .iter()
            .any(|c| c.extractor == "embedded-quote-preview")
    );
    result.persist(&store)?;
    assert_eq!(store.current_revision(&Identity::x("100")?)?, Some(a1));
    assert_eq!(store.source_counts()?.1, 2);
    Ok(())
}

#[tokio::test]
async fn traversal_limits_and_cancellation_are_explicit_in_pinned_manifest() -> Result<()> {
    let (_dir, store) = store()?;
    let graph = XGraph::new(vec![fake(vec![
        post("100", "root", vec![("200", RelationKind::RepliesTo)]),
        post("200", "parent", vec![]),
    ])]);
    let mut config = Config::default();
    let cancel = CancellationToken::new();
    config.limits.graph_nodes = 1;
    let limited = graph
        .traverse(
            &store,
            "https://x.com/a/status/100",
            &config,
            false,
            &cancel,
        )
        .await?;
    assert_eq!(limited.omissions[0].reason, "node_limit");
    let view = limited.persist(&store)?;
    assert_eq!(view.members.len(), 1);
    assert_eq!(view.omissions[0].reason, "node_limit");
    config.limits.graph_nodes = 10;
    config.limits.reply_ancestors = 0;
    let limited = graph
        .traverse(
            &store,
            "https://x.com/a/status/100",
            &config,
            false,
            &cancel,
        )
        .await?;
    assert_eq!(limited.omissions[0].reason, "reply_ancestor_limit");
    config.limits.reply_ancestors = 25;
    config.limits.graph_depth = 0;
    assert_eq!(
        graph
            .traverse(
                &store,
                "https://x.com/a/status/100",
                &config,
                false,
                &cancel
            )
            .await?
            .omissions[0]
            .reason,
        "depth_limit"
    );
    config.limits.input_bytes = 1;
    assert_eq!(
        graph
            .traverse(
                &store,
                "https://x.com/a/status/100",
                &config,
                false,
                &cancel
            )
            .await?
            .omissions[0]
            .reason,
        "byte_limit"
    );
    cancel.cancel();
    assert_eq!(
        graph
            .traverse(
                &store,
                "https://x.com/a/status/100",
                &Config::default(),
                false,
                &cancel
            )
            .await
            .unwrap_err(),
        Failure::Cancelled
    );
    Ok(())
}

#[tokio::test]
async fn silent_provider_exhausts_traversal_deadline_and_reports_omission() -> Result<()> {
    struct Silent;
    impl Provider for Silent {
        fn name(&self) -> &'static str {
            "silent-fixture"
        }
        fn fetch<'a>(
            &'a self,
            _id: &'a str,
            _max: u64,
            _cancel: &'a CancellationToken,
        ) -> BoxFuture<'a, std::result::Result<Post, Failure>> {
            std::future::pending().boxed()
        }
    }
    let (_dir, store) = store()?;
    let mut config = Config::default();
    config.limits.download_seconds = 1;
    let graph = XGraph::new(vec![Arc::new(Silent)]);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        graph.traverse(
            &store,
            "https://x.com/a/status/100",
            &config,
            false,
            &CancellationToken::new(),
        ),
    )
    .await??;
    assert_eq!(result.omissions[0].reason, "time_limit");
    assert!(result.captures.is_empty());
    Ok(())
}

#[test]
fn article_previews_and_empty_blocks_are_not_hydrated() {
    for article in [
        json!({"title":"Preview only","content":{"blocks":[]}}),
        json!({"title":"Empty text","content":{"blocks":[{"type":"unstyled","text":""}]}}),
        serde_json::Value::Null,
    ] {
        let parsed = parse_fx(
            "100",
            &json!({"tweet":{"id":"100","text":"https://x.com/i/article/100","article":article}}),
        )
        .unwrap();
        assert_eq!(parsed.status, FetchStatus::Partial);
        assert_eq!(parsed.evidence["article_hydrated"], false);
        assert!(
            parsed.evidence["limitations"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == "article_preview_only")
        );
    }
}
