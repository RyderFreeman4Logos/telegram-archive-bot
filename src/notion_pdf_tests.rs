//! DA07 actual worker + Tool protocol + local binary download proof.
use super::*;
use crate::{
    config::{Config, Notion, NotionDestination, SecretRef},
    sources::*,
    storage::Store,
};
use std::{fs::Permissions, os::unix::fs::PermissionsExt};
use tokio_util::sync::CancellationToken;
struct Fixture {
    root: tempfile::TempDir,
    store: Store,
    config: Config,
    http: crate::network::PublicHttp,
    cancel: CancellationToken,
    retain: std::sync::atomic::AtomicBool,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.cancel.cancel();
        if std::thread::panicking()
            || self.retain.load(std::sync::atomic::Ordering::Relaxed)
            || std::env::var_os("DA07_KEEP_FIXTURE").is_some()
        {
            self.root.disable_cleanup(true);
            let summary = json!({"queue":self.store.notion_list(100).ok(),"pdf":self.store.notion_pdf_availability(100).ok(),"config":self.config});
            let _ = std::fs::write(
                self.root.path().join("failure-boundary.json"),
                serde_json::to_vec_pretty(&summary).unwrap(),
            );
            eprintln!(
                "DA07 retained synthetic fixture: {}",
                self.root.path().display()
            );
        }
    }
}
impl Fixture {
    async fn new(mode: &str) -> Result<Self> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let root = tempfile::Builder::new()
            .permissions(Permissions::from_mode(0o700))
            .tempdir()?;
        std::fs::write(
            root.path().join("diagnostics-enabled"),
            b"synthetic fixture only",
        )?;
        let exe = root.path().join("ntn.py");
        std::fs::write(&exe, include_str!("../tests/fixtures/fake_ntn_pdf.py"))?;
        std::fs::set_permissions(&exe, Permissions::from_mode(0o700))?;
        let token = root.path().join("token");
        std::fs::write(&token, "SYNTHETIC_NOTION_WORKER_TOKEN")?;
        std::fs::set_permissions(&token, Permissions::from_mode(0o600))?;
        std::fs::write(root.path().join("mode"), mode)?;
        let config = Config {
            state_dir: root.path().join("store"),
            notion: Notion {
                executable: Some(exe),
                token: Some(SecretRef::File(token)),
                destination: Some(NotionDestination::Page {
                    id: "11111111-1111-1111-1111-111111111111".into(),
                }),
                retry_seconds: 1,
                ..Notion::default()
            },
            ..Config::default()
        };
        let store = Store::open(&config.state_dir)?;
        let original = if mode == "oversize" {
            let mut bytes = vec![b' '; MAX_PDF_BYTES as usize + 1];
            bytes[..8].copy_from_slice(b"%PDF-1.4");
            bytes
        } else {
            b"%PDF-1.4 canonical fixture original".to_vec()
        };
        std::fs::write(root.path().join("original.pdf"), &original)?;
        let raw = store.publish_bytes(&original)?;
        let pdf = Identity::from_url("https://example.org/paper.pdf")?;
        store.capture(&Capture {
            identity: Identity::x("10")?,
            submitted_url: Some("https://x.com/fixture/status/10".into()),
            content: Some(Content {
                body: "Original landing post".into(),
                metadata: Default::default(),
            }),
            status: FetchStatus::Complete,
            extractor: "fixture".into(),
            evidence: json!({}),
            raw_digest: None,
            relations: vec![Related {
                target: pdf.clone(),
                kind: RelationKind::Links,
            }],
        })?;
        store.capture(&Capture{identity:pdf,submitted_url:Some("https://example.org/paper.pdf".into()),content:if mode=="no_text" {None}else{Some(Content{body:"Readable original PDF text 🌍".into(),metadata:ContentMetadata{title:Some("Paper title".into()),..Default::default()}})},status:if mode=="no_text" {FetchStatus::Missing}else{FetchStatus::Complete},extractor:"pdf-text".into(),evidence:json!({"submitted_url":"https://example.org/paper.pdf","resolved_url":"https://cdn.example.org/paper.pdf?signature=temporary","retrieved_at":100,"original":{"digest":raw.digest,"mime":"application/pdf","display_name":"paper.pdf","validated":true}}),raw_digest:Some(raw.digest),relations:vec![]})?;
        let server_root = root.path().to_owned();
        let cancel = CancellationToken::new();
        let server_cancel = cancel.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let http = crate::network::PublicHttp::fixture_to(listener.local_addr()?);
        tokio::spawn(async move {
            loop {
                tokio::select! {_=server_cancel.cancelled()=>break,connection=listener.accept()=>{let Ok((mut stream,_))=connection else{break};let mut buffer=[0;4096];let _=stream.read(&mut buffer).await;let mut original=original.clone();if std::fs::read_to_string(server_root.join("mode")).is_ok_and(|v|v=="corrupt_download"){original[0]^=1;}let header=format!("HTTP/1.1 200 OK\r\nContent-Type: application/pdf\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",original.len());let _=stream.write_all(header.as_bytes()).await;let _=stream.write_all(&original).await;}}
            }
        });
        Ok(Self {
            root,
            store,
            config,
            http,
            cancel,
            retain: std::sync::atomic::AtomicBool::new(false),
        })
    }
    async fn tick(&self) -> Result<()> {
        use std::io::Write;
        let before = self.store.notion_list(100)?;
        let result = crate::notion_worker::tick_with_http(
            &self.store,
            &self.config,
            &self.cancel,
            &self.http,
        )
        .await;
        let trace = json!({"before":before,"after":self.store.notion_list(100)?,"result":result.as_ref().map(|v|*v).map_err(|e|e.to_string())});
        writeln!(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.root.path().join("dispatches.jsonl"))?,
            "{trace}"
        )?;
        result?;
        Ok(())
    }
    async fn drain(&self, count: usize) -> Result<()> {
        let attempts = |rows: Vec<Value>| {
            rows.into_iter()
                .find(|v| v["kind"] == "pdf_attachment")
                .and_then(|v| v["attempts"].as_i64())
                .unwrap_or(0)
        };
        let before = attempts(self.store.notion_list(100)?);
        for _ in 0..count {
            self.tick().await?;
            if attempts(self.store.notion_list(100)?) > before {
                return Ok(());
            }
        }
        self.retain
            .store(true, std::sync::atomic::Ordering::Relaxed);
        anyhow::bail!(
            "fixture precondition: no PDF attempt advanced after {count} ticks; retained={}; before_attempts={before}; queue={}",
            self.root.path().display(),
            serde_json::to_string(&self.store.notion_list(100)?)?
        )
    }
    fn mode(&self, mode: &str) {
        std::fs::write(self.root.path().join("mode"), mode).unwrap();
    }
    fn status(&self) -> Value {
        self.store.notion_pdf_availability(10).unwrap().remove(0)
    }
    async fn retry(&self) -> Result<()> {
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        self.drain(8).await
    }
    fn remote(&self) -> Value {
        serde_json::from_slice(&std::fs::read(self.root.path().join("remote.json")).unwrap())
            .unwrap()
    }
}
#[tokio::test]
async fn attachment_is_complete_only_after_readback_and_actual_bytes_match() -> Result<()> {
    let f = Fixture::new("normal").await?;
    f.drain(8).await?;
    let status = f.store.notion_pdf_availability(10)?;
    assert_eq!(status.len(), 1);
    assert_eq!(
        status[0]["attachment"],
        "verified",
        "upload/echo alone cannot verify an attachment: status={status:?}; queue={:?}; calls={}",
        f.store.notion_list(100)?,
        std::fs::read_to_string(f.root.path().join("calls"))?
    );
    assert_eq!(
        status[0]["verified_sha256"],
        status[0]["original"]["sha256"]
    );
    assert!(status[0]["block_id"].is_string());
    assert!(status[0]["page_id"].is_string());
    assert_eq!(f.remote()["pdf_appends"], 1);
    assert_eq!(
        status[0]["original"]["landing_urls"][0],
        "https://x.com/i/status/10"
    );
    assert_eq!(status[0]["original"]["direct_url_stability"], "temporary");
    Ok(())
}

#[tokio::test]
async fn unknown_append_reconciles_visible_file_and_never_repeats_hidden_file() -> Result<()> {
    for mode in ["append_lost", "append_lost_hidden"] {
        let f = Fixture::new(mode).await?;
        f.drain(8).await?;
        assert_eq!(
            f.remote()["pdf_appends"],
            1,
            "status={:?}; queue={:?}; calls={}",
            f.status(),
            f.store.notion_list(100)?,
            std::fs::read_to_string(f.root.path().join("calls"))?
        );
        f.retry().await?;
        if mode == "append_lost_hidden" {
            assert_eq!(f.status()["attachment"], "unknown");
            assert_eq!(f.remote()["pdf_appends"], 1);
            f.mode("normal");
            f.retry().await?;
        }
        assert_eq!(f.status()["attachment"], "verified");
        assert_eq!(f.remote()["pdf_appends"], 1);
    }
    Ok(())
}
#[tokio::test]
async fn upload_loss_and_failed_upload_resume_retained_bytes_without_extraction() -> Result<()> {
    for mode in ["send_lost", "upload_failed"] {
        let f = Fixture::new(mode).await?;
        f.drain(8).await?;
        assert_ne!(f.status()["attachment"], "verified");
        let remote = serde_json::to_string(&f.remote())?;
        assert!(remote.contains("Readable original PDF text"));
        assert!(remote.contains("DA07 availability"));
        f.mode("normal");
        f.retry().await?;
        assert_eq!(f.status()["attachment"], "verified");
        assert_eq!(f.remote()["sends"], 1);
        assert_eq!(f.remote()["pdf_appends"], 1);
    }
    Ok(())
}
#[tokio::test]
async fn size_unsupported_and_no_text_have_portable_truthful_fallbacks() -> Result<()> {
    for mode in ["oversize", "unsupported", "no_text"] {
        let f = Fixture::new(mode).await?;
        f.drain(8).await?;
        let status = f.status();
        let remote = serde_json::to_string(&f.remote())?;
        assert!(remote.contains("https://x.com/i/status/10"));
        assert!(remote.contains("https://example.org/paper.pdf"));
        assert!(remote.contains("DA07 availability"));
        assert_eq!(status["original"]["mime"], "application/pdf");
        assert_eq!(status["file_access_guaranteed"], false);
        if mode == "no_text" {
            assert_eq!(status["attachment"], "verified");
            assert_eq!(status["original"]["text_available"], false);
            assert_eq!(
                status["original"]["extraction_status"],
                "no_extractable_text"
            );
        } else {
            assert_eq!(
                status["attachment"],
                "unavailable",
                "mode={mode}; status={status:?}; queue={:?}; calls={}; errors={}",
                f.store.notion_list(100)?,
                std::fs::read_to_string(f.root.path().join("calls"))?,
                std::fs::read_to_string(f.root.path().join("errors")).unwrap_or_default()
            );
            assert!(status["gap"].is_string());
            assert_eq!(f.remote()["pdf_appends"], 0);
            assert!(remote.contains("Readable original PDF text"));
        }
    }
    Ok(())
}
#[tokio::test]
async fn byte_mismatch_is_not_availability_and_signed_url_refresh_reuses_stable_block() -> Result<()>
{
    let f = Fixture::new("corrupt_download").await?;
    f.drain(8).await?;
    assert_ne!(f.status()["attachment"], "verified");
    assert!(f.status()["verified_sha256"].is_null());
    assert_eq!(f.remote()["pdf_appends"], 1);
    f.mode("normal");
    f.retry().await?;
    let before = f.status();
    assert_eq!(before["attachment"], "verified");
    let key = before["queue_id"].as_str().unwrap();
    f.store.reconcile_notion(key, None)?;
    f.drain(2).await?;
    let after = f.status();
    assert_eq!(after["attachment"], "verified");
    assert_eq!(before["block_id"], after["block_id"]);
    assert_eq!(f.remote()["pdf_appends"], 1);
    assert!(!serde_json::to_string(&after)?.contains("files.example.org"));
    Ok(())
}
#[tokio::test]
async fn expired_unattached_upload_is_replaced_but_never_reattaches_an_uncertain_file() -> Result<()>
{
    let f = Fixture::new("attachment_failed").await?;
    f.drain(8).await?;
    assert_eq!(f.remote()["pdf_appends"], 0);
    assert_eq!(f.status()["upload"], "uploaded");
    let first = f.status()["upload_id"].clone();
    f.mode("expired");
    f.retry().await?;
    assert_eq!(f.status()["upload"], "expired");
    assert!(f.status()["upload_id"].is_null());
    f.mode("normal");
    f.retry().await?;
    assert_eq!(f.status()["attachment"], "verified");
    assert_ne!(f.status()["upload_id"], first);
    assert_eq!(f.remote()["pdf_appends"], 1);
    Ok(())
}
#[tokio::test]
async fn completed_proof_is_revoked_on_missing_file_and_operator_reconcile_is_read_only()
-> Result<()> {
    let f = Fixture::new("normal").await?;
    f.drain(8).await?;
    let status = f.status();
    assert_eq!(status["attachment"], "verified");
    let before = std::fs::read_to_string(f.root.path().join("calls"))?
        .lines()
        .count();
    f.mode("missing_attachment");
    f.store
        .reconcile_notion(status["queue_id"].as_str().unwrap(), None)?;
    f.drain(2).await?;
    assert_ne!(f.status()["attachment"], "verified");
    assert!(f.status()["verified_sha256"].is_null());
    let calls = std::fs::read_to_string(f.root.path().join("calls"))?;
    for line in calls.lines().skip(before) {
        let argv: Vec<String> = serde_json::from_str(line)?;
        assert!(
            !argv.iter().any(|v| v == "POST" || v == "PATCH"),
            "operator reconciliation must make no writes"
        );
    }
    assert_eq!(f.remote()["pdf_appends"], 1);
    Ok(())
}
#[tokio::test]
async fn old_backup_recovery_hold_and_restart_reconcile_existing_attachment() -> Result<()> {
    let mut f = Fixture::new("attachment_failed").await?;
    f.drain(8).await?;
    assert_eq!(f.remote()["pdf_appends"], 0);
    let status = f.status();
    let backup = f.root.path().join("backup");
    f.store.backup(&backup)?;
    f.mode("normal");
    f.retry().await?;
    assert_eq!(f.remote()["pdf_appends"], 1);
    let restored = f.root.path().join("restored");
    Store::restore_backup(&backup, &restored)?;
    f.store = Store::open(&restored)?;
    std::fs::write(f.root.path().join("db-root"), restored.to_str().unwrap())?;
    let calls = std::fs::read(f.root.path().join("calls"))?;
    for _ in 0..2 {
        f.tick().await?;
    }
    assert_eq!(std::fs::read(f.root.path().join("calls"))?, calls);
    assert!(f.store.recovery_paused()?);
    assert_eq!(
        f.store.read_blob(
            status["original"]["sha256"].as_str().unwrap(),
            MAX_PDF_BYTES
        )?,
        std::fs::read(f.root.path().join("original.pdf"))?
    );
    f.store.release_recovery_hold()?;
    f.drain(8).await?;
    assert_eq!(f.status()["attachment"], "verified");
    assert_eq!(f.remote()["pdf_appends"], 1);
    Ok(())
}
#[tokio::test]
async fn failed_binary_does_not_block_other_records_or_explicit_manifest_gap() -> Result<()> {
    use crate::notion::{Coverage, Provenance, Record, RecordData, RecordLink};
    let f = Fixture::new("upload_failed").await?;
    f.drain(8).await?;
    let status = f.status();
    let original: PdfOriginal = serde_json::from_value(status["original"].clone())?;
    let record = Record {
        schema_version: 1,
        id: "fixture-day-manifest".into(),
        title: "Synthetic day manifest".into(),
        captured_at: "100".into(),
        original_urls: vec![original.canonical_url.clone()],
        provenance: Provenance {
            provider: "archive".into(),
            extractor: "fixture".into(),
            retrieved_at: "100".into(),
            completeness: FetchStatus::Partial,
            original_bytes_sha256: None,
        },
        data: RecordData::DailyManifest {
            local_date: "2026-10-25".into(),
            timezone: "America/Los_Angeles".into(),
            start_utc: "2026-10-25T07:00:00Z".into(),
            end_utc: "2026-10-26T07:00:00Z".into(),
            account_ids: vec![],
            records: vec![RecordLink {
                record_id: status["queue_id"].as_str().unwrap().into(),
                source_id: original.source_id.clone(),
                revision_id: original.revision_id.clone(),
                original_url: original.canonical_url.clone(),
                notion_page_id: status["page_id"].as_str().map(str::to_owned),
            }],
            coverage: Coverage {
                complete: false,
                admitted: 1,
                published: 1,
                missing_record_ids: vec![],
                gaps: vec![format!(
                    "PDF binary unavailable: {}",
                    serde_json::to_string(&status)?
                )],
            },
        },
        content: "Readable day record; original PDF text remains independently readable.".into(),
    };
    let key = f.store.record_notion_document(&record)?;
    for _ in 0..10 {
        f.tick().await?;
        if f.store
            .notion_list(100)?
            .iter()
            .any(|v| v["publication_key"] == key && v["state"] == "complete")
        {
            break;
        }
    }
    assert!(
        f.store
            .notion_list(100)?
            .iter()
            .any(|v| v["publication_key"] == key && v["state"] == "complete")
    );
    let remote = f.remote();
    let portable = serde_json::to_string(&remote)?;
    assert!(portable.contains("PDF binary unavailable"));
    assert!(portable.contains("Readable original PDF text"));
    assert!(portable.contains("https://x.com/i/status/10"));
    assert!(!portable.contains(f.store.root().to_str().unwrap()));
    assert_ne!(f.status()["attachment"], "verified");
    Ok(())
}

#[tokio::test]
async fn old_published_pdf_admission_is_bounded_and_reuses_existing_marker() -> Result<()> {
    let f = Fixture::new("normal").await?;
    f.drain(8).await?;
    let original = f.status();
    {
        let mut conn = f.store.connection()?;
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM notion_attempts WHERE queue_id IN (SELECT id FROM notion_queue WHERE kind='pdf_attachment')",[])?;
        tx.execute("DELETE FROM notion_pdf", [])?;
        tx.execute("DELETE FROM notion_queue WHERE kind='pdf_attachment'", [])?;
        tx.execute("UPDATE notion_pdf_bootstrap SET cursor='',done=0", [])?;
        tx.commit()?;
    }
    assert_eq!(f.store.notion_pdf_availability(10)?.len(), 0);
    assert_eq!(f.store.notion_pdf_bootstrap(1)?, 1);
    assert!(f.store.notion_pdf_bootstrap(65).is_err());
    f.drain(8).await?;
    assert_eq!(f.status()["attachment"], "verified");
    assert_eq!(f.status()["block_id"], original["block_id"]);
    assert_eq!(f.remote()["pdf_appends"], 1);
    Ok(())
}
#[tokio::test]
async fn auth_gap_can_resume_explicitly_but_unknown_append_cannot_be_retried() -> Result<()> {
    let f = Fixture::new("auth_failed").await?;
    f.drain(8).await?;
    assert_ne!(f.status()["attachment"], "verified");
    let key = f.status()["queue_id"].as_str().unwrap().to_owned();
    f.mode("normal");
    f.store.retry_notion(&key)?;
    f.drain(8).await?;
    assert_eq!(f.status()["attachment"], "verified");
    let f = Fixture::new("append_lost_hidden").await?;
    f.drain(8).await?;
    assert!(
        f.store
            .retry_notion(f.status()["queue_id"].as_str().unwrap())
            .is_err()
    );
    assert_eq!(f.remote()["pdf_appends"], 1);
    Ok(())
}
