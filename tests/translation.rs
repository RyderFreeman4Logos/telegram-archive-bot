use anyhow::Result;
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use telegram_archive_bot::{
    config::Config,
    jobs::{Failure, Lease, Phase},
    sources::{Capture, Content, ContentMetadata, FetchStatus, Identity, ViewPolicy},
    storage::Store,
    telegram::Update,
    translation::{HymtTranslator, TranslationManifest, TranslationWorkflow, Translator},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};
use tokio_util::sync::CancellationToken;

struct Server {
    endpoint: String,
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(mode: &'static str) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/v1", listener.local_addr()?);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let entered = Arc::new(Notify::new());
        let notify = entered.clone();
        let release = Arc::new(Notify::new());
        let release_task = release.clone();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let count = count.clone();
                let notify = notify.clone();
                let release = release_task.clone();
                tokio::spawn(async move {
                    let mut request = vec![];
                    let mut buf = [0u8; 8192];
                    let body;
                    loop {
                        let n = socket.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buf[..n]);
                        if let Some(end) = request.windows(4).position(|s| s == b"\r\n\r\n") {
                            let header = String::from_utf8_lossy(&request[..end]);
                            let length = header
                                .lines()
                                .find_map(|line| {
                                    line.to_ascii_lowercase()
                                        .strip_prefix("content-length: ")
                                        .and_then(|v| v.parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            if request.len() >= end + 4 + length {
                                body = request[end + 4..end + 4 + length].to_vec();
                                break;
                            }
                        }
                    }
                    let post = request.starts_with(b"POST ");
                    let (status, kind, response) = if post {
                        let n = count.fetch_add(1, Ordering::SeqCst) + 1;
                        notify.notify_one();
                        if mode == "slow" && n == 1 {
                            release.notified().await;
                        }
                        if mode == "silent" {
                            let mut byte = [0u8; 1];
                            let _ = socket.read(&mut byte).await;
                            return;
                        }
                        let input: Value = serde_json::from_slice(&body).unwrap();
                        for key in [
                            "temperature",
                            "top_p",
                            "top_k",
                            "min_p",
                            "repeat_penalty",
                            "repetition_penalty",
                            "repeat_last_n",
                        ] {
                            assert!(input.get(key).is_none(), "sampler override {key}");
                        }
                        assert_eq!(input["model"], "fixture-model");
                        let streaming = input["stream"].as_bool().unwrap_or(false);
                        if mode == "error" || (mode == "second_error" && n == 2) {
                            (
                                503,
                                "application/json",
                                json!({"error":"SYNTHETIC_PRIVATE_UPSTREAM_BODY"}).to_string(),
                            )
                        } else {
                            let prompt = input["messages"][0]["content"].as_str().unwrap();
                            let source = prompt.split_once("\n\n").unwrap().1;
                            let text = if mode == "empty" {
                                String::new()
                            } else if mode == "semantic_omission" {
                                "A completed response omitting most of its source.".into()
                            } else {
                                source.replace(
                                    "English",
                                    &if mode == "counted" {
                                        format!("译文{n}")
                                    } else {
                                        "译文".into()
                                    },
                                )
                            };
                            let finish = if mode == "length" { "length" } else { "stop" };
                            let chunk = json!({"choices":[{"index":0,"delta":{"content":text},"finish_reason":null}]});
                            let terminal =
                                json!({"choices":[{"index":0,"delta":{},"finish_reason":finish}]});
                            if streaming {
                                (
                                    200,
                                    "text/event-stream",
                                    format!(
                                        "data: {chunk}\n\ndata: {terminal}\n\ndata: [DONE]\n\n"
                                    ),
                                )
                            } else {
                                (200,"application/json",json!({"choices":[{"message":{"content":text},"finish_reason":finish}]}).to_string())
                            }
                        }
                    } else if request.starts_with(b"GET /props ") {
                        let value = if mode == "unverified" {
                            json!({"invalid":true})
                        } else {
                            json!({"model_alias":"fixture-model","n_ctx":8192,"n_ctx_per_seq":8192,"n_parallel":1,"version":"fixture-1",
                            "default_generation_settings":{"temperature":0.7,"top_p":0.95,"top_k":40,"min_p":0.05,"repeat_penalty":1.1,"repeat_last_n":64}})
                        };
                        (200, "application/json", value.to_string())
                    } else {
                        (404, "application/json", "{}".into())
                    };
                    let response = format!(
                        "HTTP/1.1 {status} Fixture\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                        response.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Ok(Self {
            endpoint,
            calls,
            entered,
            release,
            task,
        })
    }
    fn count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

fn temp() -> Result<tempfile::TempDir> {
    Ok(tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"))?)
}
fn bridge() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("integrations/hymt-bridge/target/debug/archive-hymt-bridge");
    assert!(
        path.is_file(),
        "build integrations/hymt-bridge before running the integrated suite"
    );
    path
}
fn prepare(
    store: &Store,
    server: &Server,
    text: &str,
    id: i64,
    profile: &str,
    budget: u64,
) -> Result<(Lease, String)> {
    let mut config = Config::default();
    config.telegram.bot_id = 123;
    config.telegram.allowed_chats = vec![10];
    config.telegram.allowed_users = vec![20];
    config.translation.enabled = true;
    config.translation.deployment_revision = "fixture-deployment-v1".into();
    config.translation.backend = "llama_cpp".into();
    config.translation.endpoint = server.endpoint.clone();
    config.translation.model = "fixture-model".into();
    config.translation.profile = profile.into();
    config.tools.hymt_bridge = bridge();
    config.limits.llm_seconds = budget;
    config.limits.graph_nodes = 200;
    config.validate()?;
    let update: Update = serde_json::from_value(
        json!({"update_id":id,"message":{"message_id":id,"chat":{"id":10},"from":{"id":20},"text":"fixture"}}),
    )?;
    store.ingest(&config, &update, &json!({"hymt":"pinned"}))?;
    if id == 99 {
        rusqlite::Connection::open(store.root().join("archive.sqlite"))?
            .execute("UPDATE jobs SET force_run=1 WHERE update_id=99", [])?;
    }
    let lease = store.claim_phase(Phase::Fetch)?.unwrap();
    let capture = Capture {
        identity: Identity::from_url("https://example.org/translation-fixture")?,
        submitted_url: None,
        content: Some(Content {
            body: text.into(),
            metadata: ContentMetadata::default(),
        }),
        status: FetchStatus::Complete,
        extractor: "fixture".into(),
        evidence: json!({}),
        raw_digest: None,
        relations: vec![],
    };
    let revision = store.capture(&capture)?.unwrap();
    let view = store.create_view(
        &capture.identity,
        ViewPolicy {
            max_nodes: 10,
            max_depth: 10,
        },
    )?;
    store.finish_phase(
        &lease,
        &json!({"view_id":view.id()?,"partial":false}),
        Some(Phase::Metadata),
        false,
    )?;
    let lease = store.claim_phase(Phase::Metadata)?.unwrap();
    store.finish_phase(
        &lease,
        &json!({"fixture":true}),
        Some(Phase::Translate),
        false,
    )?;
    Ok((store.claim_phase(Phase::Translate)?.unwrap(), revision))
}
fn manifest(store: &Store, hash: &str) -> Result<TranslationManifest> {
    Ok(serde_json::from_slice(&store.read_blob(hash, 1_048_576)?)?)
}
fn derivatives(store: &Store) -> Result<i64> {
    Ok(
        rusqlite::Connection::open(store.root().join("archive.sqlite"))?.query_row(
            "SELECT count(*) FROM derivatives WHERE kind='translation'",
            [],
            |r| r.get(0),
        )?,
    )
}

#[tokio::test]
async fn mixed_markdown_verified_cache_profile_and_snapshot() -> Result<()> {
    let server = Server::new("ok").await?;
    let temp = temp()?;
    let store = Store::open(temp.path())?;
    let text = format!(
        "已经是中文的段落，保留原文。\n\n```rust\nlet secret = \"λ😀\";\n```\n\n{}\n\n{}",
        "English words with Unicode café and punctuation. ".repeat(60),
        "English ending paragraph. ".repeat(40)
    );
    let (lease, id) = prepare(&store, &server, &text, 1, "generic", 10)?;
    let revision = store.revision(&id)?;
    let translator = HymtTranslator {
        store: store.clone(),
    };
    let cancel = CancellationToken::new();
    let first = translator
        .translate(&lease, &revision, "zh", "generic", &cancel)
        .await?;
    let output = manifest(&store, &first)?;
    assert!(output.report.cache_verified);
    assert!(output.report.segments > 1);
    assert!(!output.partial);
    assert!(!output.semantic_fidelity_verified);
    assert_eq!(output.source_revision, id);
    assert_ne!(output.output_digest, telegram_archive_bot::digest(&text));
    let translated = String::from_utf8(store.read_blob(&output.output_digest, 1_048_576)?)?;
    assert!(translated.contains("let secret = \"λ😀\";"));
    assert!(translated.contains("已经是中文的段落"));
    assert!(output.report.preserved.iter().any(|r| r.kind == "code"));
    assert_eq!(store.revision(&id)?.content.body, text);
    let count = server.count();
    assert!(count > 1);
    let second = translator
        .translate(&lease, &revision, "zh", "generic", &cancel)
        .await?;
    assert_eq!(second, first);
    assert_eq!(server.count(), count);
    let (changed, _) = prepare(&store, &server, &text, 2, "hy_mt2_7b", 10)?;
    let third = translator
        .translate(&changed, &revision, "zh", "hy_mt2_7b", &cancel)
        .await?;
    assert_ne!(third, first);
    assert!(server.count() > count);
    let mut mutated = lease.clone();
    mutated.config.translation.profile = "hy_mt2_7b".into();
    assert!(
        translator
            .translate(&mutated, &revision, "zh", "hy_mt2_7b", &cancel)
            .await
            .is_err()
    );
    assert_eq!(lease.config.translation.profile, "generic");
    store.integrity()?;
    Ok(())
}

#[tokio::test]
async fn unverified_identity_does_not_reuse_application_cache() -> Result<()> {
    let server = Server::new("unverified").await?;
    let temp = temp()?;
    let store = Store::open(temp.path())?;
    let (lease, id) = prepare(
        &store,
        &server,
        "English text for a cache check.",
        1,
        "generic",
        10,
    )?;
    let translator = HymtTranslator {
        store: store.clone(),
    };
    let revision = store.revision(&id)?;
    let cancel = CancellationToken::new();
    let first = translator
        .translate(&lease, &revision, "zh", "generic", &cancel)
        .await?;
    let calls = server.count();
    assert!(!manifest(&store, &first)?.report.cache_verified);
    translator
        .translate(&lease, &revision, "zh", "generic", &cancel)
        .await?;
    assert_eq!(server.count(), calls * 2);
    Ok(())
}

#[tokio::test]
async fn truncation_failure_empty_and_semantic_claims_are_distinct() -> Result<()> {
    for mode in [
        "length",
        "error",
        "empty",
        "semantic_omission",
        "second_error",
    ] {
        let server = Server::new(mode).await?;
        let temp = temp()?;
        let store = Store::open(temp.path())?;
        let text = if mode == "second_error" {
            "English long segment text. ".repeat(200)
        } else {
            "English original text with several facts and https://example.org/original".into()
        };
        let (lease, id) = prepare(&store, &server, &text, 1, "generic", 10)?;
        let revision = store.revision(&id)?;
        let result = HymtTranslator {
            store: store.clone(),
        }
        .translate(
            &lease,
            &revision,
            "zh",
            "generic",
            &CancellationToken::new(),
        )
        .await;
        match mode {
            "length" => {
                let result = manifest(&store, &result?)?;
                assert!(result.partial);
                assert!(!result.report.retained.is_empty());
                assert!(!result.semantic_fidelity_verified);
                assert_eq!(server.count(), 1);
            }
            "semantic_omission" => {
                let result = manifest(&store, &result?)?;
                assert!(!result.semantic_fidelity_verified);
            }
            _ => {
                assert!(
                    result.is_err(),
                    "{mode} unexpectedly published {:?}",
                    result.as_ref().ok().map(|hash| manifest(&store, hash))
                );
                let error = result.unwrap_err();
                if mode == "error" || mode == "second_error" {
                    assert_eq!(error.downcast_ref::<Failure>(), Some(&Failure::Unavailable));
                }
                assert!(!format!("{error:?}").contains("SYNTHETIC_PRIVATE_UPSTREAM_BODY"));
                assert_eq!(derivatives(&store)?, 0);
                if mode == "second_error" {
                    assert!(server.count() >= 2);
                } else {
                    assert_eq!(server.count(), 1);
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn silent_bridge_cancellation_timeout_and_no_complete_publication() -> Result<()> {
    for cancel_run in [true, false] {
        let server = Server::new("silent").await?;
        let temp = temp()?;
        let store = Store::open(temp.path())?;
        let (lease, id) = prepare(
            &store,
            &server,
            "English silent upstream fixture.",
            1,
            "generic",
            if cancel_run { 30 } else { 1 },
        )?;
        let token = CancellationToken::new();
        let child = token.clone();
        let entered = server.entered.clone();
        let trigger = cancel_run.then(|| {
            tokio::spawn(async move {
                entered.notified().await;
                child.cancel();
            })
        });
        let result = HymtTranslator {
            store: store.clone(),
        }
        .translate(&lease, &store.revision(&id)?, "zh", "generic", &token)
        .await
        .unwrap_err();
        if let Some(trigger) = trigger {
            tokio::time::timeout(Duration::from_secs(5), trigger).await??;
        }
        assert_eq!(
            result.downcast_ref::<Failure>(),
            Some(if cancel_run {
                &Failure::Cancelled
            } else {
                &Failure::Timeout
            })
        );
        assert_eq!(derivatives(&store)?, 0);
        if cancel_run {
            assert_eq!(server.count(), 1);
        } else {
            assert!(server.count() <= 1);
        }
    }
    Ok(())
}

#[tokio::test]
async fn adk_calls_host_translator_and_commits_phase() -> Result<()> {
    let server = Server::new("ok").await?;
    let temp = temp()?;
    let store = Store::open(temp.path())?;
    let (lease, _) = prepare(&store, &server, "English workflow input.", 1, "generic", 10)?;
    let workflow = TranslationWorkflow {
        store: store.clone(),
        translator: Arc::new(HymtTranslator {
            store: store.clone(),
        }),
    };
    workflow.run(&lease, CancellationToken::new()).await?;
    assert_eq!(
        store.phase_outputs(&lease.job.id)?["translate"]["translations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store.claim_phase(Phase::Download)?.unwrap().job.id,
        lease.job.id
    );
    Ok(())
}

#[tokio::test]
async fn bridge_detaches_configuration_before_first_segment() -> Result<()> {
    let server = Server::new("slow").await?;
    let temp = temp()?;
    let store = Store::open(temp.path())?;
    let text = "English immutable snapshot for several segments. ".repeat(80);
    let (lease, id) = prepare(&store, &server, &text, 1, "generic", 30)?;
    let executing = lease.clone();
    let revision = store.revision(&id)?;
    let worker_store = store.clone();
    let mut worker = tokio::spawn(async move {
        HymtTranslator {
            store: worker_store,
        }
        .translate(
            &executing,
            &revision,
            "zh",
            "generic",
            &CancellationToken::new(),
        )
        .await
    });
    tokio::select! {
        _ = server.entered.notified() => (),
        result = &mut worker => anyhow::bail!("bridge exited before first POST: {:?}", result),
        _ = tokio::time::sleep(Duration::from_secs(25)) => anyhow::bail!("bridge did not reach the first POST within its fixture startup budget"),
    }
    let mut replaced = 0;
    for entry in fs::read_dir(store.root().join("work"))? {
        let path = entry?.path().join("job.toml");
        if path.is_file() {
            fs::write(path, "invalid operator reload = [")?;
            replaced += 1;
        }
    }
    assert_eq!(replaced, 1);
    server.release.notify_one();
    let hash = worker.await??;
    let result = manifest(&store, &hash)?;
    assert!(result.report.segments > 1);
    assert_eq!(result.profile, "generic");
    assert_eq!(result.config_id, lease.job.config_id);
    Ok(())
}

#[tokio::test]
async fn concurrent_verified_translation_generates_once_and_force_uses_fresh_run() -> Result<()> {
    let server = Server::new("counted").await?;
    let root = temp()?;
    let store = Store::open(root.path())?;
    let text = (0..30)
        .map(|i| format!("English source passage number {i} with observable output.\n\n"))
        .collect::<String>();
    let (first, id) = prepare(&store, &server, &text, 1, "generic", 30)?;
    let (second, _) = prepare(&store, &server, &text, 2, "generic", 30)?;
    assert_eq!(first.job.config_id, second.job.config_id);
    let revision = store.revision(&id)?;
    let translator = HymtTranslator {
        store: store.clone(),
    };
    let cancel = CancellationToken::new();
    let (a, b) = tokio::join!(
        translator.translate(&first, &revision, "zh", "generic", &cancel),
        translator.translate(&second, &revision, "zh", "generic", &cancel)
    );
    let a = a?;
    let b = b?;
    let m = manifest(&store, &a)?;
    let other = manifest(&store, &b)?;
    let left = store.read_blob(&m.output_digest, 1_048_576)?;
    let right = store.read_blob(&other.output_digest, 1_048_576)?;
    assert_eq!(
        m.output_digest,
        other.output_digest,
        "calls={} first_report={} second_report={} first_output={} second_output={}",
        server.count(),
        serde_json::to_string(&m)?,
        serde_json::to_string(&other)?,
        String::from_utf8_lossy(&left),
        String::from_utf8_lossy(&right)
    );
    assert!(m.report.cache_verified);
    let generated = server.count();
    assert_eq!(generated, m.report.segments);
    let (forced, _) = prepare(&store, &server, &text, 99, "generic", 30)?;
    let fresh = translator
        .translate(&forced, &revision, "zh", "generic", &cancel)
        .await?;
    assert_ne!(a, fresh);
    assert_eq!(server.count(), generated * 2);
    store.verify_blob(&a)?;
    translator
        .translate(&forced, &revision, "zh", "generic", &cancel)
        .await?;
    assert_eq!(server.count(), generated * 2);
    Ok(())
}
