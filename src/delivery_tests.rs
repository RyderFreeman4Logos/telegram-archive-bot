use super::*;
use crate::{
    audio::{AudioFinalizer, AudioInput, Repair},
    jobs::{Failure, Phase},
    telegram::Update,
};
use std::{
    collections::VecDeque,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};
struct Request {
    method: String,
    body: Vec<u8>,
    json: Option<Value>,
}
struct Server {
    base: String,
    requests: Arc<Mutex<Vec<Request>>>,
    entered: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(modes: Vec<&'static str>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base = format!("http://{}", listener.local_addr()?);
        let requests = Arc::new(Mutex::new(vec![]));
        let captured = requests.clone();
        let entered = Arc::new(Notify::new());
        let notify = entered.clone();
        let modes = Arc::new(Mutex::new(VecDeque::from(modes)));
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let captured = captured.clone();
                let notify = notify.clone();
                let modes = modes.clone();
                tokio::spawn(async move {
                    let mut request = vec![];
                    let mut buf = [0u8; 8192];
                    let (header, body) = loop {
                        let n = socket.read(&mut buf).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        request.extend_from_slice(&buf[..n]);
                        assert!(request.len() < 2_000_000);
                        if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                            let h = String::from_utf8_lossy(&request[..end]).into_owned();
                            let length = h
                                .lines()
                                .find_map(|s| {
                                    s.to_ascii_lowercase()
                                        .strip_prefix("content-length: ")
                                        .and_then(|s| s.parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            if request.len() >= end + 4 + length {
                                break (h, request[end + 4..end + 4 + length].to_vec());
                            }
                        }
                    };
                    let method = header
                        .lines()
                        .next()
                        .unwrap()
                        .split(' ')
                        .nth(1)
                        .unwrap()
                        .rsplit('/')
                        .next()
                        .unwrap()
                        .to_string();
                    let json = serde_json::from_slice::<Value>(&body).ok();
                    let text = json
                        .as_ref()
                        .map(|v| v["text"].clone())
                        .unwrap_or(Value::Null);
                    captured.lock().unwrap().push(Request {
                        method: method.clone(),
                        body,
                        json,
                    });
                    notify.notify_one();
                    let mode = modes.lock().unwrap().pop_front().unwrap_or("ok");
                    if mode == "disconnect" {
                        return;
                    }
                    if mode == "silent" {
                        let mut b = [0; 1];
                        let _ = socket.read(&mut b).await;
                        return;
                    }
                    let field = match method.as_str() {
                        "sendAudio" => "audio",
                        "sendVoice" => "voice",
                        "sendDocument" => "document",
                        _ => "text",
                    };
                    let mut media =
                        json!({"file_id":"CACHED_FIXTURE","file_unique_id":"UNIQUE_FIXTURE"});
                    if field == "audio" || field == "voice" {
                        media["duration"] = json!(3);
                        media["mime_type"] = json!(if field == "audio" {
                            "audio/mpeg"
                        } else {
                            "audio/ogg"
                        });
                    }
                    if mode == "bad_duration" {
                        media["duration"] = json!(30);
                    }
                    if mode == "missing_id" {
                        media.as_object_mut().unwrap().remove("file_unique_id");
                    }
                    let mut message = json!({"message_id":7,"chat":{"id":2}});
                    message[field] = if field == "text" { text } else { media };
                    let (code, value) = match mode {
                        "429" => (
                            429,
                            json!({"ok":false,"error_code":429,"parameters":{"retry_after":60}}),
                        ),
                        "503" => (503, json!({"ok":false,"error_code":503})),
                        "cached_rejected" => (
                            400,
                            json!({"ok":false,"error_code":400,"description":"wrong file identifier"}),
                        ),
                        _ => (200, json!({"ok":true,"result":message})),
                    };
                    let body = if mode == "malformed" {
                        "{broken".into()
                    } else {
                        value.to_string()
                    };
                    let response = format!(
                        "HTTP/1.1 {code} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Ok(Self {
            base,
            requests,
            entered,
            task,
        })
    }
    fn worker(&self, store: &Store, bot: i64) -> Result<DeliveryWorker> {
        Ok(DeliveryWorker {
            store: store.clone(),
            telegram: Telegram::new("SYNTHETIC_TOKEN".into())?.fixture_base(self.base.clone()),
            bot_id: bot,
        })
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}
fn config() -> Config {
    let mut c = Config::default();
    c.telegram.bot_id = 1;
    c.telegram.allowed_chats = vec![2];
    c.telegram.allowed_users = vec![3];
    c.limits.media_seconds = 30;
    c.limits.send_seconds = 1;
    c
}
fn add(store: &Store, c: &Config, update: i64, text: &str) -> Result<Lease> {
    let u: Update = serde_json::from_value(
        json!({"update_id":update,"message":{"message_id":update,"chat":{"id":2},"from":{"id":3},"text":text}}),
    )?;
    store.ingest(c, &u, &json!({"fixture":"v1"}))?;
    Ok(store.claim_phase(Phase::Fetch)?.unwrap())
}
fn fixture() -> Result<(tempfile::TempDir, Store, Lease)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target");
    let temp = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(root)?;
    let store = Store::open(temp.path())?;
    let lease = add(&store, &config(), 1, "fixture")?;
    Ok((temp, store, lease))
}
fn scope() -> Scope {
    Scope {
        bot_id: 1,
        chat_id: 2,
        user_id: 3,
    }
}

#[tokio::test]
async fn uncertain_failure_notice_cannot_block_a_successfully_retried_result() -> Result<()> {
    for outcome in ["unknown", "terminal"] {
        let (root, store, existing) = fixture()?;
        store.finish_phase(&existing, &json!({}), None, false)?;
        let mut c = config();
        c.telegram.enabled = true;
        let lease = add(&store, &c, 2, "Source to retry")?;
        store.fail_phase(&lease, Failure::InvalidOutput)?;
        store.connection()?.execute(
            "UPDATE outbox SET state=CASE WHEN ordinal=-1 THEN 'confirmed' ELSE ?1 END",
            [outcome],
        )?;
        store.retry_job(scope(), &lease.job.id, Phase::Fetch)?;
        let retry = store.claim_phase(Phase::Fetch)?.unwrap();
        store.enqueue_deliveries(&retry, &[document(&store)?])?;
        store.finish_phase(&retry, &json!({}), None, false)?;
        let reopened = Store::open(root.path())?;
        let server = Server::new(vec![]).await?;
        assert!(
            server
                .worker(&reopened, 1)?
                .send_next(&CancellationToken::new())
                .await?,
            "failure feedback must not block a retried final result"
        );
        assert_eq!(server.requests.lock().unwrap().len(), 1);
        assert_eq!(server.requests.lock().unwrap()[0].method, "sendDocument");
    }
    Ok(())
}

#[tokio::test]
async fn uncertain_progress_notice_does_not_block_final_output() -> Result<()> {
    let (_root, store, lease) = fixture()?;
    store.connection()?.execute(
        "INSERT INTO outbox(id,job_id,kind,payload,state,ordinal) VALUES('notice',?1,'text',?2,'unknown',-1)",
        params![lease.job.id, serde_json::to_string(&Delivery::Text { text: "Queued".into() })?],
    )?;
    store.enqueue_deliveries(&lease, &[document(&store)?])?;
    let server = Server::new(vec![]).await?;
    assert!(
        server
            .worker(&store, 1)?
            .send_next(&CancellationToken::new())
            .await?,
        "uncertain progress feedback must not prevent delivery of the result"
    );
    assert_eq!(server.requests.lock().unwrap()[0].method, "sendDocument");
    Ok(())
}

#[test]
fn later_delivery_groups_append_after_feedback_and_stay_idempotent() -> Result<()> {
    let (_root, store, lease) = fixture()?;
    let feedback = [Delivery::Text {
        text: "Queued".into(),
    }];
    let notice = store.enqueue_deliveries(&lease, &feedback)?;
    let outputs = [
        document(&store)?,
        Delivery::Text {
            text: "Complete".into(),
        },
    ];
    let result = store.enqueue_deliveries(&lease, &outputs);
    assert!(
        result.is_ok(),
        "final outputs must append after progress feedback"
    );
    let ids = result?;
    assert_eq!(ids, store.enqueue_deliveries(&lease, &outputs)?);
    assert_eq!(notice, store.enqueue_deliveries(&lease, &feedback)?);
    let rows: Vec<(String, i64)> = store
        .connection()?
        .prepare("SELECT id,ordinal FROM outbox ORDER BY ordinal")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    assert_eq!(
        rows,
        vec![
            (notice[0].clone(), 0),
            (ids[0].clone(), 1),
            (ids[1].clone(), 2)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn deliveries_reply_to_persisted_message_not_update_id_after_reopen() -> Result<()> {
    let (_root, store, _) = fixture()?;
    let incoming: Update = serde_json::from_value(json!({
        "update_id": 44,
        "message": {"message_id": 314, "chat": {"id": 2}, "from": {"id": 3}, "text": "Reply source"}
    }))?;
    store.ingest(&config(), &incoming, &json!({}))?;
    let lease = store.claim_phase(Phase::Fetch)?.unwrap();
    let deliveries = [
        Delivery::Text {
            text: "Complete".into(),
        },
        document(&store)?,
    ];
    store.enqueue_deliveries(&lease, &deliveries)?;
    let reopened = Store::open(store.root())?;
    let server = Server::new(vec![]).await?;
    let worker = server.worker(&reopened, 1)?;
    for _ in &deliveries {
        assert!(worker.send_next(&CancellationToken::new()).await?);
    }
    let requests = server.requests.lock().unwrap();
    assert_eq!(
        requests[0].json.as_ref().unwrap()["reply_parameters"],
        json!({"message_id": 314, "allow_sending_without_reply": true})
    );
    let multipart = String::from_utf8_lossy(&requests[1].body);
    assert!(multipart.contains("name=\"reply_parameters\""));
    assert!(multipart.contains("\"message_id\":314"));
    Ok(())
}
fn document(store: &Store) -> Result<Delivery> {
    Ok(Delivery::Document {
        blob_digest: store.publish_bytes("# Synthetic\n全文".as_bytes())?.digest,
        name: "example.md".into(),
        mime: "text/markdown".into(),
        caption: "<b>Literal & safe</b>".into(),
    })
}
async fn audio(store: &Store, l: &Lease, voice: bool) -> Result<Delivery> {
    let path = store.root().join("synthetic.mp3");
    let result = std::process::Command::new("/usr/bin/ffmpeg")
        .args([
            "-v",
            "error",
            "-nostdin",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:duration=3",
            "-c:a",
            "libmp3lame",
            "-b:a",
            "96k",
        ])
        .arg(&path)
        .output()?;
    assert!(result.status.success());
    let raw = store.publish_bytes(&fs::read(&path)?)?;
    fs::remove_file(path)?;
    let inputs = [AudioInput {
        blob_digest: raw.digest,
        parent_id: "synthetic-producer".into(),
        producer_tools: json!({"fixture":"real_ffmpeg"}),
        order: 0,
        subtitle_digest: None,
    }];
    let artifact = AudioFinalizer {
        store: store.clone(),
    }
    .finalize(
        l,
        "fixture",
        &inputs,
        if voice {
            Profile::VoiceNote
        } else {
            Profile::AudioTrack
        },
        Repair::Transcode,
        &CancellationToken::new(),
    )
    .await?
    .remove(0);
    Ok(Delivery::Audio {
        artifact_id: artifact.artifact_id,
        title: "Title <&>".into(),
        performer: "Synthetic".into(),
        caption: "Full content".into(),
    })
}
#[tokio::test]
async fn ordered_audio_document_text_receipts_and_literal_unicode_caption() -> Result<()> {
    let (_t, store, l) = fixture()?;
    let a = audio(&store, &l, false).await?;
    let d = document(&store)?;
    let deliveries = [
        a,
        d,
        Delivery::Text {
            text: "<b>Literal 😀 & text</b>".into(),
        },
    ];
    let ids = store.enqueue_deliveries(&l, &deliveries)?;
    assert_eq!(ids, store.enqueue_deliveries(&l, &deliveries)?);
    let server = Server::new(vec![]).await?;
    let worker = server.worker(&store, 1)?;
    let token = CancellationToken::new();
    for id in &ids {
        assert!(worker.send_next(&token).await?);
        assert_eq!(store.outbox_for(scope(), id)?.state, "confirmed");
    }
    assert!(!worker.send_next(&token).await?);
    let requests = server.requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|r| r.method.as_str())
            .collect::<Vec<_>>(),
        vec!["sendAudio", "sendDocument", "sendMessage"]
    );
    let body = String::from_utf8_lossy(&requests[0].body);
    assert!(body.contains("audio/mpeg"));
    assert!(body.contains("name=\"duration\"\r\n\r\n3"));
    assert!(!body.contains("parse_mode"));
    assert!(String::from_utf8_lossy(&requests[1].body).contains("<b>Literal & safe</b>"));
    assert_eq!(caption(&"😀".repeat(700)).encode_utf16().count(), 1023);
    Ok(())
}
#[tokio::test]
async fn cache_is_bot_and_presentation_scoped_and_rejection_uploads_without_producers() -> Result<()>
{
    let (_t, store, l) = fixture()?;
    let a = audio(&store, &l, false).await?;
    let ids = store.enqueue_deliveries(&l, std::slice::from_ref(&a))?;
    let server = Server::new(vec!["ok", "cached_rejected", "ok", "ok"]).await?;
    let worker = server.worker(&store, 1)?;
    let token = CancellationToken::new();
    worker.send_next(&token).await?;
    assert_eq!(store.outbox_for(scope(), &ids[0])?.state, "confirmed");
    let next = add(&store, &config(), 2, "same audio")?;
    let ids2 = store.enqueue_deliveries(&next, std::slice::from_ref(&a))?;
    worker.send_next(&token).await?;
    assert_eq!(store.outbox_for(scope(), &ids2[0])?.state, "retryable");
    worker.send_next(&token).await?;
    assert_eq!(store.outbox_for(scope(), &ids2[0])?.state, "confirmed");
    let mut other = config();
    other.telegram.bot_id = 9;
    let l2 = add(&store, &other, 3, "another bot")?;
    store.enqueue_deliveries(&l2, &[a])?;
    server.worker(&store, 9)?.send_next(&token).await?;
    let requests = server.requests.lock().unwrap();
    assert!(requests[0].json.is_none());
    assert_eq!(
        requests[1].json.as_ref().unwrap()["audio"],
        "CACHED_FIXTURE"
    );
    assert!(requests[2].json.is_none() && requests[3].json.is_none());
    assert_eq!(requests.len(), 4);
    assert_eq!(
        store
            .connection()?
            .query_row("SELECT count(*) FROM model_attempts", [], |r| r
                .get::<_, i64>(0))?,
        0
    );
    Ok(())
}
#[tokio::test]
async fn voice_profile_and_invalid_raw_or_wrong_receipts_cannot_confirm() -> Result<()> {
    let (_t, store, l) = fixture()?;
    let raw = store.publish_bytes(b"not finalized")?.digest;
    assert!(
        store
            .enqueue_deliveries(
                &l,
                &[Delivery::Audio {
                    artifact_id: raw,
                    title: String::new(),
                    performer: String::new(),
                    caption: String::new()
                }]
            )
            .is_err()
    );
    let a = audio(&store, &l, true).await?;
    let ids = store.enqueue_deliveries(&l, &[a])?;
    let server = Server::new(vec!["bad_duration"]).await?;
    server
        .worker(&store, 1)?
        .send_next(&CancellationToken::new())
        .await?;
    let status = store.outbox_for(scope(), &ids[0])?;
    assert_eq!(status.state, "unknown");
    assert_eq!(status.error.as_deref(), Some("invalid_success_receipt"));
    assert_eq!(server.requests.lock().unwrap()[0].method, "sendVoice");
    assert_eq!(
        store
            .connection()?
            .query_row("SELECT count(*) FROM telegram_file_ids", [], |r| r
                .get::<_, i64>(0))?,
        0
    );
    assert!(store.retry_delivery(scope(), &ids[0], false).is_err());
    store.retry_delivery(scope(), &ids[0], true)?;
    assert!(
        store
            .outbox_for(
                Scope {
                    user_id: 999,
                    ..scope()
                },
                &ids[0]
            )
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn rate_limits_transient_rejections_and_missing_identifiers_retain_history() -> Result<()> {
    for mode in ["429", "503", "missing_id"] {
        let (_t, store, l) = fixture()?;
        let ids = store.enqueue_deliveries(&l, &[document(&store)?])?;
        let server = Server::new(vec![mode, "ok"]).await?;
        let worker = server.worker(&store, 1)?;
        let token = CancellationToken::new();
        worker.send_next(&token).await?;
        let status = store.outbox_for(scope(), &ids[0])?;
        assert_eq!(
            status.state,
            if mode == "missing_id" {
                "unknown"
            } else {
                "retryable"
            }
        );
        if mode == "429" {
            let next = add(&store, &config(), 2, "another message during cooldown")?;
            store.enqueue_deliveries(&next, &[document(&store)?])?;
        }
        assert!(!worker.send_next(&token).await?);
        assert_eq!(server.count(), 1);
        if mode == "429" {
            let until: i64 = store.connection()?.query_row(
                "SELECT available_at FROM outbox WHERE id=?1",
                [&ids[0]],
                |r| r.get(0),
            )?;
            assert!(until >= now() + 59);
        }
        if mode == "429" {
            // Advance both persisted clocks after asserting their original wait.
            store
                .connection()?
                .execute("UPDATE telegram_cooldowns SET until=0", [])?;
            store
                .connection()?
                .execute("UPDATE outbox SET available_at=0 WHERE id=?1", [&ids[0]])?;
        }
        store.retry_delivery(scope(), &ids[0], mode == "missing_id")?;
        worker.send_next(&token).await?;
        assert_eq!(store.outbox_for(scope(), &ids[0])?.state, "confirmed");
        assert_eq!(
            store.connection()?.query_row(
                "SELECT count(*) FROM delivery_attempts WHERE outbox_id=?1",
                [&ids[0]],
                |r| r.get::<_, i64>(0)
            )?,
            2
        );
    }
    Ok(())
}
#[tokio::test]
async fn timeout_disconnect_invalid_json_and_cancelled_transmission_are_unknown() -> Result<()> {
    for mode in ["silent", "disconnect", "malformed", "cancel"] {
        let (_t, store, l) = fixture()?;
        let ids = store.enqueue_deliveries(&l, &[document(&store)?])?;
        let server = Server::new(vec![if mode == "cancel" { "silent" } else { mode }]).await?;
        let worker = server.worker(&store, 1)?;
        let token = CancellationToken::new();
        let child = token.clone();
        let entered = server.entered.clone();
        let trigger = tokio::spawn(async move {
            if mode == "cancel" {
                entered.notified().await;
                child.cancel();
            }
        });
        worker.send_next(&token).await?;
        trigger.await?;
        assert_eq!(store.outbox_for(scope(), &ids[0])?.state, "unknown");
        assert!(!worker.send_next(&CancellationToken::new()).await?);
        assert_eq!(server.count(), 1);
        let err = store.outbox_for(scope(), &ids[0])?.error.unwrap();
        assert!(!err.contains("SYNTHETIC_TOKEN"));
    }
    Ok(())
}
#[tokio::test]
async fn crash_after_real_fixture_response_does_not_replay_external_send() -> Result<()> {
    let (_t, store, l) = fixture()?;
    let ids = store.enqueue_deliveries(&l, &[document(&store)?])?;
    let server = Server::new(vec![]).await?;
    let exe = std::env::current_exe()?;
    let root = store.root().to_owned();
    let base = server.base.clone();
    let result = tokio::task::spawn_blocking(move || {
        std::process::Command::new(exe)
            .args(["--exact", "delivery::tests::crash_child", "--nocapture"])
            .env("ARCHIVE_TEST_CHILD_ROOT", root)
            .env("ARCHIVE_TEST_CHILD_BASE", base)
            .env("ARCHIVE_TEST_CRASH_AFTER_SEND_RESPONSE", "1")
            .output()
    })
    .await??;
    assert_eq!(result.status.code(), Some(93));
    assert_eq!(server.count(), 1);
    assert_eq!(store.outbox_for(scope(), &ids[0])?.state, "sending");
    store
        .connection()?
        .execute("UPDATE outbox SET lease_until=0 WHERE id=?1", [&ids[0]])?;
    assert!(
        !server
            .worker(&store, 1)?
            .send_next(&CancellationToken::new())
            .await?
    );
    assert_eq!(store.outbox_for(scope(), &ids[0])?.state, "unknown");
    assert_eq!(server.count(), 1);
    Ok(())
}
#[tokio::test]
async fn crash_child() -> Result<()> {
    let Some(root) = std::env::var_os("ARCHIVE_TEST_CHILD_ROOT") else {
        return Ok(());
    };
    let store = Store::open(Path::new(&root))?;
    let telegram = Telegram::new("SYNTHETIC_TOKEN".into())?
        .fixture_base(std::env::var("ARCHIVE_TEST_CHILD_BASE")?);
    DeliveryWorker {
        store,
        telegram,
        bot_id: 1,
    }
    .send_next(&CancellationToken::new())
    .await?;
    anyhow::bail!("crash boundary was not reached")
}

#[tokio::test]
async fn claim_releases_database_writer_before_waiting_for_publication_lock() -> Result<()> {
    let (_t, store, l) = fixture()?;
    let original = store.enqueue_deliveries(&l, &[document(&store)?])?;
    store.claim_delivery(1)?.unwrap();
    store.connection()?.execute(
        "UPDATE outbox SET lease_until=0 WHERE id=?1",
        [&original[0]],
    )?;
    let next = add(&store, &config(), 2, "second job")?;
    store.enqueue_deliveries(&next, &[document(&store)?])?;
    let lock = store.publication_lock()?;
    let child = store.clone();
    let thread = std::thread::spawn(move || child.claim_delivery(1));
    let mut committed = false;
    for _ in 0..100 {
        if store.outbox_for(scope(), &original[0])?.state == "unknown" {
            committed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let writer = if committed {
        store
            .connection()?
            .execute(
                "INSERT INTO projection_events(kind,entity_id) VALUES('fixture','lock-order')",
                [],
            )
            .is_ok()
    } else {
        false
    };
    drop(lock);
    let result = tokio::task::spawn_blocking(move || thread.join().unwrap()).await??;
    assert!(
        committed && writer,
        "claim held the database writer while waiting for publication"
    );
    assert!(result.is_some());
    Ok(())
}

#[tokio::test]
async fn identical_bytes_with_different_document_presentations_do_not_share_file_ids() -> Result<()>
{
    let (_t, store, l) = fixture()?;
    let markdown = document(&store)?;
    let mut plain = markdown.clone();
    if let Delivery::Document { mime, .. } = &mut plain {
        *mime = "text/plain".into();
    }
    store.enqueue_deliveries(&l, std::slice::from_ref(&markdown))?;
    let server = Server::new(vec![]).await?;
    let worker = server.worker(&store, 1)?;
    let token = CancellationToken::new();
    worker.send_next(&token).await?;
    let second = add(&store, &config(), 2, "plain presentation")?;
    store.enqueue_deliveries(&second, &[plain])?;
    worker.send_next(&token).await?;
    let third = add(&store, &config(), 3, "same Markdown presentation")?;
    store.enqueue_deliveries(&third, &[markdown])?;
    worker.send_next(&token).await?;
    let requests = server.requests.lock().unwrap();
    assert!(requests[0].json.is_none() && requests[1].json.is_none());
    assert_eq!(
        requests[2].json.as_ref().unwrap()["document"],
        "CACHED_FIXTURE"
    );
    Ok(())
}
#[tokio::test]
async fn competing_delivery_workers_select_one_attempt_and_local_corruption_sends_nothing()
-> Result<()> {
    let (_t, store, l) = fixture()?;
    let payload = document(&store)?;
    let ids = store.enqueue_deliveries(&l, std::slice::from_ref(&payload))?;
    let server = Server::new(vec![]).await?;
    let a = server.worker(&store, 1)?;
    let b = server.worker(&store, 1)?;
    let cancel = CancellationToken::new();
    let (x, y) = tokio::join!(a.send_next(&cancel), b.send_next(&cancel));
    let x = x?;
    let y = y?;
    assert!(x || y);
    assert_eq!(server.count(), 1);
    assert_eq!(store.outbox_for(scope(), &ids[0])?.attempts, 1);
    let next = add(&store, &config(), 2, "corrupt retained input")?;
    let ids = store.enqueue_deliveries(&next, std::slice::from_ref(&payload))?;
    if let Delivery::Document { blob_digest, .. } = payload {
        fs::write(store.blob_path(&blob_digest)?, "tampered")?;
    }
    assert!(!a.send_next(&cancel).await?);
    assert_eq!(server.count(), 1);
    assert_eq!(store.outbox_for(scope(), &ids[0])?.state, "terminal");
    Ok(())
}

#[test]
fn legacy_sending_rows_without_a_lease_recover_unknown_and_keep_logical_order() -> Result<()> {
    let temp = tempfile::Builder::new()
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))?;
    let path = temp.path().join("archive.sqlite");
    let conn = rusqlite::Connection::open(&path)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    conn.execute_batch(include_str!("../migrations/001_canonical.sql"))?;
    conn.execute_batch(include_str!("../migrations/002_revision_quality.sql"))?;
    conn.execute_batch(include_str!("../migrations/003_ingress.sql"))?;
    conn.execute_batch(include_str!("../migrations/004_job_recovery.sql"))?;
    conn.execute_batch(include_str!("../migrations/005_derivatives.sql"))?;
    conn.execute_batch(include_str!("../migrations/006_immutable_artifacts.sql"))?;
    let config = serde_json::to_string(&config())?;
    let id = digest(format!("{config}\n{{}}"));
    conn.execute(
        "INSERT INTO config_snapshots VALUES(?1,?2,'{}')",
        params![id, config],
    )?;
    conn.execute("INSERT INTO inbox VALUES(1,1,2,3,'[]')", [])?;
    conn.execute("INSERT INTO jobs(id,bot_id,update_id,ordinal,request,config_id,state) VALUES('legacy',1,1,0,?1,?2,'complete')",params![serde_json::to_string(&crate::telegram::Request::Text{text:"legacy".into()})?,id])?;
    let payload = serde_json::to_string(&Delivery::Text {
        text: "legacy text".into(),
    })?;
    conn.execute("INSERT INTO outbox(id,job_id,kind,payload,state) VALUES('old-send','legacy','text',?1,'sending')",[&payload])?;
    conn.execute("INSERT INTO outbox(id,job_id,kind,payload,state) VALUES('old-pending','legacy','text',?1,'pending')",[&payload])?;
    conn.pragma_update(None, "user_version", 6)?;
    drop(conn);
    let store = Store::open(temp.path())?;
    assert!(store.claim_delivery(1)?.is_none());
    assert_eq!(store.outbox_for(scope(), "old-send")?.state, "unknown");
    assert_eq!(store.outbox_for(scope(), "old-pending")?.state, "pending");
    assert_eq!(
        store.connection()?.query_row(
            "SELECT ordinal FROM outbox WHERE id='old-pending'",
            [],
            |r| r.get::<_, u64>(0)
        )?,
        1
    );
    Ok(())
}
