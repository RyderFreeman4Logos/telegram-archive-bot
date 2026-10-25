//! Service dispatcher. The database owns scheduling; adapters own no delivery side effects.
use crate::{
    audio::{Profile, Repair},
    config::{AudioProfile, Config, ConfigManager},
    delivery::{Delivery, DeliveryWorker, enqueue_tx},
    documents::{Documents, Extraction},
    jobs::{Failure, Lease, Phase, fence, finish_in_tx, supervise_lease},
    network::PublicHttp,
    sources::{Capture, Content, ContentMetadata, FetchStatus, Identity, ViewPolicy},
    storage::{Store, now},
    telegram::{Operation, Request, Telegram, Update},
    transforms::{AudioSet, DownloadRequest, NarrationRequest},
    translation::{HymtTranslator, TranslationManifest, TranslationWorkflow},
    workflows::{MetadataManifest, MetadataWorkflow, configured_model},
    x::{Traversal, XGraph},
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{RwLock, Semaphore},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

const PHASES: [Phase; 7] = [
    Phase::Fetch,
    Phase::Metadata,
    Phase::Translate,
    Phase::Download,
    Phase::Synthesize,
    Phase::Finalize,
    Phase::Archive,
];

impl Store {
    pub fn accept_extraction(&self, lease: &Lease, extraction: &Extraction) -> Result<Value> {
        self.verify_blob(&extraction.original.digest)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        fence(&tx, lease, now())?;
        self.capture_in_tx(&tx, &extraction.capture)?;
        if extraction.capture.extractor == "yt-dlp-probe-v1" {
            let native: crate::media::MediaMetadata =
                serde_json::from_value(extraction.capture.evidence["native"].clone())?;
            ensure!(
                extraction.capture.identity
                    == Identity {
                        provider: format!("media:{}", native.extractor.to_ascii_lowercase()),
                        external_id: native.id
                    },
                Failure::InvalidInput
            );
            let url = extraction.capture.evidence["submitted_url"]
                .as_str()
                .ok_or(Failure::InvalidInput)?;
            crate::network::source_url(url)?;
            tx.execute("INSERT INTO source_aliases VALUES(?1,?2,'yt_dlp_native_v1') ON CONFLICT DO NOTHING",rusqlite::params![extraction.capture.identity.id(),url])?;
            tx.execute("UPDATE projection_events SET group_key=(SELECT group_key FROM projection_events WHERE kind='observation' ORDER BY id DESC LIMIT 1) WHERE kind='alias' AND entity_id=json_array(?1,?2,'yt_dlp_native_v1') AND group_key IS NULL",rusqlite::params![extraction.capture.identity.id(),url])?;
        }
        let view = self.create_view_in_tx(
            &tx,
            &extraction.capture.identity,
            ViewPolicy {
                max_nodes: lease.config.limits.graph_nodes,
                max_depth: lease.config.limits.graph_depth,
            },
        )?;
        let output = json!({"view_id":view.id()?,"partial":extraction.capture.status!=FetchStatus::Complete,"original":extraction.original,"text_digest":extraction.text_digest});
        let next = if extraction.text_digest.is_some() {
            Phase::Metadata
        } else {
            Phase::Finalize
        };
        finish_in_tx(
            &tx,
            lease,
            &output,
            Some(next),
            extraction.capture.status != FetchStatus::Complete,
            now(),
        )?;
        tx.commit()?;
        Ok(output)
    }
    pub fn accept_traversal(&self, lease: &Lease, traversal: &Traversal) -> Result<Value> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        fence(&tx, lease, now())?;
        let view = traversal.persist_in_tx(self, &tx)?;
        let partial = !view.omissions.is_empty()
            || view.members.iter().any(|m| m.revision_id.is_none())
            || traversal
                .captures
                .iter()
                .any(|c| c.status != FetchStatus::Complete);
        let output = json!({"view_id":view.id()?,"partial":partial});
        let next = if view.members.iter().any(|m| m.revision_id.is_some()) {
            Phase::Metadata
        } else {
            Phase::Finalize
        };
        finish_in_tx(&tx, lease, &output, Some(next), partial, now())?;
        tx.commit()?;
        Ok(output)
    }
    fn finish_deliveries(
        &self,
        lease: &Lease,
        deliveries: &[Delivery],
        output: &Value,
    ) -> Result<()> {
        for delivery in deliveries {
            delivery.validate(self, &lease.config)?;
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        fence(&tx, lease, now())?;
        let ids = enqueue_tx(&tx, &lease.job.id, deliveries)?;
        let mut output = output.clone();
        output["deliveries"] = json!(ids);
        finish_in_tx(&tx, lease, &output, Some(Phase::Archive), false, now())?;
        tx.commit()?;
        Ok(())
    }
}

fn telegram(config: &Config) -> Result<Telegram> {
    Telegram::new(
        config
            .telegram
            .token
            .as_ref()
            .ok_or(Failure::InvalidInput)?
            .read()?,
    )
}
fn narration_profile(config: &Config) -> Profile {
    match config.telegram.narration_mode {
        AudioProfile::VoiceNote => Profile::VoiceNote,
        AudioProfile::AudioTrack => Profile::AudioTrack,
    }
}
fn media_url(url: &str) -> bool {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_owned))
        .is_some_and(|h| {
            [
                "youtube.com",
                "youtu.be",
                "vimeo.com",
                "soundcloud.com",
                "bilibili.com",
                "b23.tv",
                "xiaoyuzhoufm.com",
            ]
            .iter()
            .any(|d| h == *d || h.ends_with(&format!(".{d}")))
        })
}
fn manifest<T: serde::de::DeserializeOwned>(store: &Store, id: &str, lease: &Lease) -> Result<T> {
    Ok(serde_json::from_slice(
        &store.read_blob(id, lease.config.limits.input_bytes)?,
    )?)
}
fn view(store: &Store, lease: &Lease) -> Result<crate::sources::View> {
    let outputs = store.phase_outputs(&lease.job.id)?;
    store.view(
        outputs["fetch"]["view_id"]
            .as_str()
            .ok_or(Failure::InvalidInput)?,
    )
}

async fn fetch(
    store: &Store,
    lease: &Lease,
    relay: &Path,
    cancel: &CancellationToken,
) -> Result<()> {
    if let Request::Transform { operation, source } = &lease.job.request {
        ensure!(
            match operation {
                Operation::Translate => lease.config.translation.enabled,
                Operation::Tts => lease.config.synthesis.enabled,
                Operation::Summarize => lease.config.model.enabled,
            },
            Failure::ToolMissing
        );
        ensure!(
            !matches!(source.as_ref(), Request::Url { url } if media_url(url)),
            Failure::InvalidInput
        );
    }
    let docs = Documents {
        store: store.clone(),
        config: lease.config.clone(),
        http: PublicHttp::default(),
    };
    match lease.job.request.source() {
        Request::Transform { .. } => anyhow::bail!(Failure::InvalidInput),
        Request::Command { .. } => {
            store.execute_command(lease)?;
        }
        Request::Url { url } => {
            let identity = Identity::from_url(url)?;
            if identity.provider == "x" {
                let traversal = configured_graph(lease)?
                    .traverse(store, url, &lease.config, lease.job.force, cancel)
                    .await?;
                store.accept_traversal(lease, &traversal)?;
            } else if media_url(url) && lease.config.sources.media_enabled {
                let native = crate::media::MediaDownloader {
                    store: store.clone(),
                    relay_executable: relay.to_owned(),
                }
                .probe(lease, url, cancel)
                .await?;
                let identity = Identity {
                    provider: format!("media:{}", native.extractor.to_ascii_lowercase()),
                    external_id: native.id.clone(),
                };
                let body = format!(
                    "# {}\n\nMedia provider: {}\nMedia ID: {}\n\nProvider metadata only; this is not a transcript.\n",
                    native.title.as_deref().unwrap_or("Media source"),
                    native.extractor,
                    native.id
                );
                let raw = store.publish_bytes(&serde_json::to_vec(&native)?)?;
                let text = store.publish_bytes(body.as_bytes())?;
                let extraction = Extraction {
                    capture: Capture {
                        identity,
                        submitted_url: None,
                        content: Some(Content {
                            body,
                            metadata: ContentMetadata {
                                title: native.title.clone(),
                                ..Default::default()
                            },
                        }),
                        status: FetchStatus::Complete,
                        extractor: "yt-dlp-probe-v1".into(),
                        evidence: json!({"native":native,"submitted_url":url,"content_scope":"provider_metadata_only"}),
                        raw_digest: Some(raw.digest),
                        relations: vec![],
                    },
                    original: crate::documents::Original {
                        digest: text.digest.clone(),
                        mime: "text/plain".into(),
                        display_name: "media-source.txt".into(),
                        validated: true,
                    },
                    text_digest: Some(text.digest),
                };
                store.accept_extraction(lease, &extraction)?;
            } else {
                store.accept_extraction(lease, &docs.url(url, cancel).await?)?;
            }
        }
        Request::Text { text } => {
            let extraction = docs
                .attachment(
                    &format!("text-{}", crate::digest(text)),
                    "message.md",
                    text.as_bytes(),
                    cancel,
                )
                .await?;
            store.accept_extraction(lease, &extraction)?;
        }
        Request::Document {
            file_id,
            unique_id,
            name,
            size,
        } => {
            ensure!(
                *size <= lease.config.limits.input_bytes,
                Failure::InvalidInput
            );
            let bytes = telegram(&lease.config)?
                .download_document(file_id, lease.config.limits.input_bytes, cancel)
                .await?;
            store.accept_extraction(
                lease,
                &docs.attachment(unique_id, name, &bytes, cancel).await?,
            )?;
        }
        Request::LocalDocument { digest, name, .. } => {
            let bytes = store.read_blob(digest, lease.config.limits.input_bytes)?;
            let mut extraction = docs.attachment(digest, name, &bytes, cancel).await?;
            extraction.capture.identity = Identity {
                provider: "local_document".into(),
                external_id: digest.clone(),
            };
            store.accept_extraction(lease, &extraction)?;
        }
    }
    Ok(())
}

async fn execute(
    store: &Store,
    lease: &Lease,
    relay: &Path,
    cancel: &CancellationToken,
) -> Result<()> {
    match lease.job.phase {
        Phase::Fetch => fetch(store, lease, relay, cancel).await?,
        Phase::Metadata => {
            if !lease.config.model.enabled {
                store.finish_phase(
                    lease,
                    &json!({"enabled":false}),
                    Some(Phase::Translate),
                    false,
                )?;
            } else {
                let pinned = view(store, lease)?;
                let ids = pinned
                    .members
                    .iter()
                    .filter_map(|m| m.revision_id.clone())
                    .collect::<Vec<_>>();
                let workflow = MetadataWorkflow {
                    store: store.clone(),
                    model: configured_model(lease)?,
                };
                let hash = workflow.run(lease, &ids, cancel).await?;
                store.finish_phase(
                    lease,
                    &json!({"enabled":true,"metadata_digest":hash,"view_id":pinned.id()?}),
                    Some(Phase::Translate),
                    false,
                )?;
            }
        }
        Phase::Translate => {
            TranslationWorkflow {
                store: store.clone(),
                translator: Arc::new(HymtTranslator {
                    store: store.clone(),
                }),
            }
            .run(lease, cancel.clone())
            .await?
        }
        Phase::Download => {
            let mut artifacts = vec![];
            let mut candidates = vec![];
            if let Request::Url { url } = lease.job.request.source()
                && lease.config.sources.media_enabled
                && media_url(url)
            {
                let pinned = view(store, lease)?;
                let revision = pinned
                    .members
                    .iter()
                    .find_map(|m| m.revision_id.clone())
                    .ok_or(Failure::InvalidInput)?;
                let output = store
                    .download_audio(
                        lease,
                        &DownloadRequest {
                            revision,
                            url: url.clone(),
                            playlist_limit: None,
                            profile: Profile::AudioTrack,
                            repair: Repair::Transcode,
                        },
                        relay,
                        cancel,
                    )
                    .await?;
                artifacts = output.artifacts;
                candidates = output.candidates;
            }
            store.finish_phase(
                lease,
                &json!({"artifacts":artifacts,"candidates":candidates}),
                Some(Phase::Synthesize),
                false,
            )?;
        }
        Phase::Synthesize => {
            let mut sets = vec![];
            if lease.config.synthesis.enabled
                && !matches!(lease.job.request.source(),Request::Url{url} if lease.config.sources.media_enabled&&media_url(url))
            {
                let outputs = store.phase_outputs(&lease.job.id)?;
                let translated = outputs
                    .get("translate")
                    .and_then(|o| o["translations"].as_array())
                    .cloned()
                    .unwrap_or_default();
                let mut inputs = vec![];
                if translated.is_empty() {
                    for member in view(store, lease)?.members {
                        if let Some(id) = member.revision_id {
                            let revision = store.revision(&id)?;
                            let blob = store.publish_bytes(revision.content.body.as_bytes())?;
                            inputs.push((id, blob.digest, "original"));
                        }
                    }
                } else {
                    for hash in translated {
                        let m: TranslationManifest =
                            manifest(store, hash.as_str().ok_or(Failure::InvalidOutput)?, lease)?;
                        inputs.push((m.source_revision, m.output_digest, "translation"));
                    }
                }
                for (revision, input_digest, kind) in inputs {
                    sets.push(
                        store
                            .narrate(
                                lease,
                                &NarrationRequest {
                                    revision,
                                    input_digest,
                                    input_kind: kind.into(),
                                    view_id: None,
                                    profile: narration_profile(&lease.config),
                                    repair: Repair::Transcode,
                                },
                                cancel,
                            )
                            .await?,
                    );
                }
            }
            store.finish_phase(lease, &json!({"audio":sets}), Some(Phase::Finalize), false)?;
        }
        Phase::Finalize => {
            let outputs = store.phase_outputs(&lease.job.id)?;
            let pinned = view(store, lease)?;
            let operation = match lease.job.request {
                Request::Transform { operation, .. } => Some(operation),
                _ => None,
            };
            let mut text = String::new();
            let mut title = "Archive".to_owned();
            for member in &pinned.members {
                if let Some(id) = &member.revision_id {
                    let revision = store.revision(id)?;
                    text.push_str(&format!(
                        "# Source {}\n\n{}\n\n",
                        member.source_id, revision.content.body
                    ));
                } else {
                    text.push_str(&format!("Unavailable source: {}\n\n", member.source_id));
                }
            }
            let mut summary = String::new();
            if let Some(hash) = outputs
                .get("metadata")
                .and_then(|o| o["metadata_digest"].as_str())
            {
                let m: MetadataManifest = manifest(store, hash, lease)?;
                title = m.result.title.clone();
                summary = format!("# {}\n\n{}\n", m.result.title, m.result.summary);
                text = format!("{summary}\n{text}");
            }
            let mut translation = String::new();
            if let Some(hashes) = outputs
                .get("translate")
                .and_then(|o| o["translations"].as_array())
            {
                for hash in hashes {
                    let m: TranslationManifest =
                        manifest(store, hash.as_str().ok_or(Failure::InvalidOutput)?, lease)?;
                    let bytes =
                        store.read_blob(&m.output_digest, lease.config.limits.input_bytes)?;
                    translation.push_str(std::str::from_utf8(&bytes)?);
                    translation.push_str("\n\n");
                }
            }
            let mut deliveries = vec![];
            let mut artifacts = vec![];
            let mut documents = vec![];
            if operation.is_none() && !text.is_empty() {
                documents.push((text, "", "Archived original"));
            }
            if (operation.is_none() || operation == Some(Operation::Translate))
                && !translation.is_empty()
            {
                documents.push((translation, "-translation", "Translation"));
            }
            if operation == Some(Operation::Summarize) && !summary.is_empty() {
                documents.push((summary, "-summary", "Generated summary"));
            }
            for (text, suffix, caption) in documents {
                ensure!(
                    text.len() as u64 <= lease.config.limits.input_bytes,
                    Failure::InvalidOutput
                );
                let blob = store.publish_bytes(text.as_bytes())?;
                deliveries.push(Delivery::Document {
                    blob_digest: blob.digest,
                    name: format!(
                        "{}{suffix}.md",
                        crate::documents::display_name(&title)
                            .chars()
                            .take(45)
                            .collect::<String>()
                    ),
                    mime: "text/markdown".into(),
                    caption: caption.into(),
                });
            }
            if operation == Some(Operation::Summarize)
                && let Request::LocalDocument {
                    digest,
                    name,
                    rename,
                } = lease.job.request.source()
            {
                deliveries.push(Delivery::Document {
                    blob_digest: digest.clone(),
                    name: if *rename {
                        format!(
                            "{}.md",
                            crate::documents::display_name(&title)
                                .chars()
                                .take(45)
                                .collect::<String>()
                        )
                    } else {
                        name.clone()
                    },
                    mime: "text/markdown".into(),
                    caption: "Retained original input".into(),
                });
            }
            if operation.is_none()
                && let Some(original) = outputs["fetch"].get("original")
            {
                let original: crate::documents::Original =
                    serde_json::from_value(original.clone())?;
                if original.mime == "application/pdf" && original.validated {
                    deliveries.push(Delivery::Document {
                        blob_digest: original.digest,
                        name: original.display_name,
                        mime: original.mime,
                        caption: "Retained original PDF".into(),
                    });
                }
            }
            if let Some(ids) = outputs
                .get("download")
                .and_then(|o| o["artifacts"].as_array())
            {
                for id in ids {
                    artifacts.push(id.as_str().ok_or(Failure::InvalidOutput)?.to_owned());
                }
            }
            if let Some(sets) = outputs
                .get("synthesize")
                .and_then(|o| o["audio"].as_array())
            {
                for set in sets {
                    artifacts.extend(serde_json::from_value::<AudioSet>(set.clone())?.artifacts);
                }
            }
            for id in &artifacts {
                deliveries.push(Delivery::Audio{artifact_id:id.clone(),title:"Archived audio".into(),performer:"Telegram Archive".into(),caption:"Validated audio; original content completeness is not independently verified.".into()});
            }
            if deliveries.is_empty() {
                ensure!(operation.is_none(), Failure::InvalidOutput);
                deliveries.push(Delivery::Text {
                    text: "Source unavailable; retained a partial observation.".into(),
                });
            }
            store.finish_deliveries(
                lease,
                &deliveries,
                &json!({"view_id":pinned.id()?,"artifacts":artifacts}),
            )?;
        }
        Phase::Archive => {
            if lease.config.archive.directory.is_some() {
                store.project(&lease.config, 100, cancel).await?;
            }
            let outputs = store.phase_outputs(&lease.job.id)?;
            store.finish_phase(lease,&json!({"view_id":outputs["fetch"]["view_id"],"artifacts":outputs["finalize"]["artifacts"],"deliveries":outputs["finalize"]["deliveries"],"processing_complete":true,"delivery_status":"inspect-outbox"}),None,false)?;
        }
    }
    Ok(())
}

pub async fn step(
    store: &Store,
    phase: Phase,
    relay: &Path,
    cancel: &CancellationToken,
) -> Result<bool> {
    let claimed = if phase == Phase::Fetch {
        store.claim_source()?
    } else {
        store.claim_phase(phase)?
    };
    let Some(lease) = claimed else {
        return Ok(false);
    };
    run_lease(store, lease, relay, cancel).await?;
    Ok(true)
}
pub(crate) async fn step_job(
    store: &Store,
    id: &str,
    relay: &Path,
    cancel: &CancellationToken,
) -> Result<bool> {
    let Some(lease) = store.claim_job_id(id)? else {
        return Ok(false);
    };
    run_lease(store, lease, relay, cancel).await?;
    Ok(true)
}
async fn control_step(store: &Store, relay: &Path, cancel: &CancellationToken) -> Result<bool> {
    let Some(lease) = store.claim_control()? else {
        return Ok(false);
    };
    run_lease(store, lease, relay, cancel).await?;
    Ok(true)
}
async fn run_lease(
    store: &Store,
    lease: Lease,
    relay: &Path,
    cancel: &CancellationToken,
) -> Result<()> {
    crate::operator::log("phase_started", Some(&lease), json!({}));
    let started = Instant::now();
    let executing = &lease;
    let result = supervise_lease(
        store.clone(),
        lease.clone(),
        cancel.clone(),
        |token| async move {
            execute(store, executing, relay, &token).await.map_err(|e| {
                e.downcast_ref::<Failure>()
                    .copied()
                    .unwrap_or(Failure::Internal)
            })
        },
    )
    .await;
    match result {
        Ok(()) => crate::operator::log(
            "phase_completed",
            Some(&lease),
            json!({"elapsed_ms":started.elapsed().as_millis()}),
        ),
        Err(failure) => {
            if failure != Failure::LeaseLost {
                store.fail_phase(&lease, failure)?;
            }
            crate::operator::log(
                "phase_failed",
                Some(&lease),
                json!({"failure":failure,"elapsed_ms":started.elapsed().as_millis()}),
            );
        }
    }
    Ok(())
}

/// Explicit offline replay: persists synthetic updates and computes, never polls or sends.
pub async fn replay(
    config: &Config,
    updates: &[Update],
    cancel: &CancellationToken,
) -> Result<Value> {
    ensure!(
        !config.telegram.enabled && !config.archive.push,
        "replay requires Telegram and archive push disabled"
    );
    let store = Store::open(&config.state_dir)?;
    ensure!(!store.recovery_paused()?, "recovery hold blocks replay");
    let relay = std::env::current_exe()?;
    for update in updates {
        store.ingest(
            config,
            update,
            &json!({"application":env!("CARGO_PKG_VERSION"),"mode":"offline-replay"}),
        )?;
    }
    for _ in 0..config.limits.graph_nodes.saturating_mul(7).max(7) {
        let mut worked = control_step(&store, &relay, cancel).await?;
        for phase in PHASES {
            worked |= step(&store, phase, &relay, cancel).await?;
        }
        if !worked {
            return crate::operator::metrics(&store);
        }
    }
    anyhow::bail!("replay step budget exhausted")
}

pub async fn serve(path: PathBuf, shutdown: CancellationToken) -> Result<()> {
    let _stop_on_error = shutdown.clone().drop_guard();
    let mut initial = Config::load(&path)?;
    disable_missing_optional(&mut initial);
    initial.check_secrets()?;
    let store = Store::open(&initial.state_dir)?;
    ensure!(
        !store.recovery_paused()?,
        "recovery hold blocks live activation"
    );
    let _service_lock = {
        use fs2::FileExt;
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(store.root().join("service.lock"))?;
        file.try_lock_exclusive()?;
        file
    };
    let diagnostics = crate::operator::doctor(&initial, &store, &shutdown).await?;
    ensure!(
        diagnostics["ready"] == true,
        "doctor reports an enabled capability is unavailable"
    );
    let config = Arc::new(RwLock::new(ConfigManager::new(initial)?));
    let telegram_enabled = config.read().await.snapshot().telegram.enabled;
    let relay = std::env::current_exe()?;
    // One worker per phase; shared semaphores also bound sibling phases using the same resource.
    let limits = config.read().await.snapshot().limits.clone();
    let downloads = Arc::new(Semaphore::new(limits.downloads));
    let models = Arc::new(Semaphore::new(limits.models));
    let syntheses = Arc::new(Semaphore::new(limits.syntheses));
    let finalizers = Arc::new(Semaphore::new(limits.finalizers));
    let mut workers = JoinSet::new();
    workers.spawn(crate::notion_worker::serve(
        store.clone(),
        config.clone(),
        shutdown.clone(),
    ));
    {
        let store = store.clone();
        let relay = relay.clone();
        let cancel = shutdown.clone();
        workers.spawn(async move{while !cancel.is_cancelled(){if control_step(&store,&relay,&cancel).await.is_err(){crate::operator::log("control_error",None,json!({}));}tokio::select!{_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_millis(100))=>{}}}Ok::<_,anyhow::Error>(())});
    }
    for phase in PHASES {
        let store = store.clone();
        let cancel = shutdown.clone();
        let relay = relay.clone();
        let resource = match phase {
            Phase::Fetch | Phase::Download => Some(downloads.clone()),
            Phase::Metadata | Phase::Translate => Some(models.clone()),
            Phase::Synthesize => Some(syntheses.clone()),
            _ => None,
        };
        let finalizers = finalizers.clone();
        workers.spawn(async move{while !cancel.is_cancelled(){let _permit=if let Some(resource)=&resource{tokio::select!{_=cancel.cancelled()=>break,p=resource.acquire()=>Some(p?)}}else{None};let _finalizer=if matches!(phase,Phase::Download|Phase::Synthesize){tokio::select!{_=cancel.cancelled()=>break,p=finalizers.acquire()=>Some(p?)}}else{None};if let Err(_error)=step(&store,phase,&relay,&cancel).await{crate::operator::log("worker_error",None,json!({"phase":phase}));}tokio::select!{_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_millis(200))=>{}}}Ok::<_,anyhow::Error>(())});
    }
    if telegram_enabled {
        let store = store.clone();
        let config = config.clone();
        let cancel = shutdown.clone();
        workers.spawn(async move{while !cancel.is_cancelled(){let snapshot=config.read().await.snapshot();let count=telegram(&snapshot)?.poll_once(&store,&snapshot,&json!({"application":env!("CARGO_PKG_VERSION")}),&cancel).await;if count.is_err(){crate::operator::log("poll_error",None,json!({"failure":"transport-or-envelope"}));tokio::select!{_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(2))=>{}}}}Ok::<_,anyhow::Error>(())});
    }
    if telegram_enabled {
        let store = store.clone();
        let config = config.clone();
        let cancel = shutdown.clone();
        workers.spawn(async move{while !cancel.is_cancelled(){let snapshot=config.read().await.snapshot();let worker=DeliveryWorker{store:store.clone(),telegram:telegram(&snapshot)?,bot_id:snapshot.telegram.bot_id};if worker.send_next(&cancel).await.is_err(){crate::operator::log("delivery_error",None,json!({"failure":"inspect-outbox"}));}tokio::select!{_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_millis(200))=>{}}}Ok::<_,anyhow::Error>(())});
    }
    {
        let store = store.clone();
        let config = config.clone();
        let cancel = shutdown.clone();
        workers.spawn(async move {
            let mut last_snapshot:Option<Arc<Config>>=None;
            let mut last_tick:Option<Instant>=None;
            while !cancel.is_cancelled() {
                let snapshot=config.read().await.snapshot();
                if snapshot.daily.enabled && (last_snapshot.as_ref().is_none_or(|old|!Arc::ptr_eq(old,&snapshot))
                    || last_tick.is_none_or(|t|t.elapsed()>=Duration::from_secs(snapshot.daily.interval_seconds))) {
                    match store.daily_tick(&snapshot,now()) {
                        Ok(report)=>crate::operator::log("daily_planner_tick",None,serde_json::to_value(report)?),
                        Err(_)=>crate::operator::log("daily_planner_error",None,json!({"reason":"inspect_daily_state_and_config"})),
                    }
                    last_snapshot=Some(snapshot.clone());last_tick=Some(Instant::now());
                }
                if !snapshot.daily.enabled {last_snapshot=None;}
                tokio::select!{_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_secs(1))=>{}}
            }
            Ok::<_,anyhow::Error>(())
        });
    }
    workers.spawn(crate::daily_capture::run_with(
        store.clone(),
        config.clone(),
        Arc::new(crate::daily_capture::ConfiguredCollector),
        shutdown.clone(),
    ));
    let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    let mut interval = tokio::time::interval(Duration::from_secs(60));
    crate::operator::log(
        "service_started",
        None,
        json!({"telegram_enabled":telegram_enabled}),
    );
    loop {
        tokio::select! {_=shutdown.cancelled()=>break,_=hup.recv()=>{let mut manager=config.write().await;let current=manager.snapshot();let next=Config::load(&path).map(|mut n|{disable_missing_optional(&mut n);n});if next.as_ref().is_ok_and(|n|n.limits.downloads==limits.downloads&&n.limits.models==limits.models&&n.limits.syntheses==limits.syntheses&&n.limits.finalizers==limits.finalizers&&n.limits.deliveries==limits.deliveries&&n.telegram.enabled==current.telegram.enabled&&store.validate_daily_config(n).is_ok())&&next.is_ok_and(|n|manager.activate(n).is_ok()){graph_cache().lock().map_err(|_|Failure::Internal)?.clear();crate::operator::log("reload_applied",None,json!({"config_fingerprint":manager.snapshot().fingerprint()?}));}else{crate::operator::log("reload_rejected",None,json!({"reason":"invalid-config-or-admission-change-requires-restart"}));}},_=interval.tick()=>crate::operator::log("metrics",None,crate::operator::metrics(&store)?),result=workers.join_next()=>{shutdown.cancel();if let Some(result)=result{result??;}break;}}
    }
    shutdown.cancel();
    let deadline = Duration::from_secs(limits.shutdown_seconds + 2);
    if tokio::time::timeout(deadline, async {
        while let Some(result) = workers.join_next().await {
            result??;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .is_err()
    {
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }
    crate::operator::log("service_stopped", None, json!({"live_activation":true}));
    Ok(())
}

fn disable_missing_optional(config: &mut Config) {
    let available = |path: &Path| crate::transforms::executable(path).is_ok();
    for (name, enabled, present) in [
        (
            "synthesis",
            &mut config.synthesis.enabled,
            available(&config.tools.md2audio)
                && available(&config.tools.ffmpeg)
                && available(&config.tools.ffprobe),
        ),
        (
            "translation",
            &mut config.translation.enabled,
            available(&config.tools.hymt_bridge),
        ),
        (
            "media",
            &mut config.sources.media_enabled,
            available(&config.tools.yt_dlp)
                && available(&config.tools.bubblewrap)
                && available(&config.tools.ffmpeg)
                && available(&config.tools.ffprobe),
        ),
        (
            "browser",
            &mut config.sources.browser_enabled,
            crate::browser::system_path(&config.tools.browser).is_ok()
                && available(&config.tools.browser)
                && available(&config.tools.bubblewrap),
        ),
    ] {
        if *enabled && !present {
            *enabled = false;
            crate::operator::log(
                "capability_disabled",
                None,
                json!({"capability":name,"reason":"missing-optional-executable"}),
            );
        }
    }
}

#[cfg(test)]
#[path = "runtime_tests.rs"]
mod tests;

fn graph_cache() -> &'static std::sync::Mutex<std::collections::BTreeMap<String, Arc<XGraph>>> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<std::collections::BTreeMap<String, Arc<XGraph>>>,
    > = std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}
fn configured_graph(lease: &Lease) -> Result<Arc<XGraph>> {
    let mut cache = graph_cache().lock().map_err(|_| Failure::Internal)?;
    if let Some(graph) = cache.get(&lease.job.config_id) {
        return Ok(graph.clone());
    }
    let graph = Arc::new(XGraph::configured(&lease.config)?);
    if cache.len() >= 4 {
        let stale = cache
            .iter()
            .find(|(_, graph)| Arc::strong_count(graph) == 1)
            .map(|(key, _)| key.clone());
        if let Some(key) = stale {
            cache.remove(&key);
        }
    }
    if cache.len() < 4 {
        cache.insert(lease.job.config_id.clone(), graph.clone());
    }
    Ok(graph)
}
