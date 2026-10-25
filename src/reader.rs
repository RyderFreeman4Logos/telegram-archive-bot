//! DA09 / #76: provider-neutral immutable day manifests and external reader handoffs.
//! No model, provider or publication transport runs in this module.
use crate::{
    config::Config,
    digest,
    notion::{Coverage, Provenance, Record, RecordData},
    storage::Store,
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[path = "reader_availability.rs"]
mod availability;
#[path = "reader_export.rs"]
mod export;
#[path = "reader_store.rs"]
mod store;
pub use export::PortableExport;
#[path = "reader_checkpoint.rs"]
mod checkpoint;
pub use checkpoint::{
    EvidenceKind, LayerCheckpoint, LayerState, OutputRef, ReadEvidence, ReadState,
    ReaderProvenance, SavedCheckpoint, SourceRead,
};

pub const CONTRACT: &str = "daily-reader-v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReaderConfig {
    pub enabled: bool,
    pub sop: ReaderSop,
    /// Affected-window construction has an explicit admission cap, never silent truncation.
    pub max_activities: usize,
    pub max_windows: usize,
    pub export_bytes: usize,
}
impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sop: ReaderSop::default(),
            max_activities: 10_000,
            max_windows: 1000,
            export_bytes: 100_000_000,
        }
    }
}
impl ReaderConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=100_000).contains(&self.max_activities)
                && (1..=1000).contains(&self.max_windows)
                && (16_384..=1_000_000_000).contains(&self.export_bytes),
            "invalid reader bounds"
        );
        ensure!(
            !self.sop.id.is_empty()
                && !self.sop.version.is_empty()
                && self.sop.id.len() <= 256
                && self.sop.version.len() <= 256,
            "invalid reader SOP identity"
        );
        if let Some(url) = &self.sop.page_url {
            crate::network::source_url(url)?;
        }
        if let Some(id) = &self.sop.page_id {
            ensure!(
                crate::notion::notion_id(id).is_some(),
                "invalid reader SOP page ID"
            );
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReaderSop {
    pub id: String,
    pub version: String,
    pub page_id: Option<String>,
    pub page_url: Option<String>,
}
impl Default for ReaderSop {
    fn default() -> Self {
        Self {
            id: "x-daily-briefing-v1".into(),
            version: "1.0.0".into(),
            page_id: None,
            page_url: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentRef {
    pub document_id: String,
    pub record_id: String,
    pub lookup_title: String,
    pub page_id: Option<String>,
    pub page_url: Option<String>,
    pub verified: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRef {
    pub source_id: String,
    pub revision_id: Option<String>,
    pub original_urls: Vec<String>,
    pub role: String,
    pub capture_status: crate::sources::FetchStatus,
    pub missing_reason: Option<String>,
    pub text_sha256: Option<String>,
    pub publication: Option<DocumentRef>,
    pub enrichment: Option<SourceEnrichment>,
}
/// Serial DA04/DA07/DA08 producers may update only this typed local evidence seam.
/// It never changes a pinned source revision or asserts that a person read it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceEnrichment {
    pub source_id: String,
    pub revision_id: Option<String>,
    pub attachments: Vec<Attachment>,
    pub gaps: Vec<String>,
    pub failure_report_urls: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture: Option<CaptureAvailability>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureAvailability {
    pub state: String,
    pub reason: Option<String>,
    pub cycle: i64,
    pub attempts: u32,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attachment {
    pub original_url: String,
    pub landing_url: Option<String>,
    pub direct_url: Option<String>,
    pub upload_state: String,
    pub attachment_state: String,
    pub verified_sha256: Option<String>,
    pub page_id: Option<String>,
    pub block_id: Option<String>,
    pub gap: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityEntry {
    pub activity_id: String,
    pub observation_id: String,
    pub window_ids: Vec<String>,
    pub disposition: String,
    pub observation: crate::activities::ActivityObservation,
    pub sources: Vec<SourceRef>,
    pub context_omissions: Vec<crate::sources::Omission>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowEvidence {
    pub window_id: String,
    pub account_id: String,
    pub config_id: String,
    pub provider: String,
    pub state: String,
    pub reason: Option<String>,
    pub attempts: u32,
    pub evidence: Value,
    #[serde(default)]
    pub failure_report_urls: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub contract: String,
    pub scope: String,
    pub window_id: Option<String>,
    pub local_date: String,
    pub timezone: String,
    pub utc_start: i64,
    pub utc_end: i64,
    pub sop: ReaderSop,
    pub sop_configured: bool,
    pub windows: Vec<WindowEvidence>,
    pub counts: BTreeMap<String, u64>,
    pub index: Vec<DocumentRef>,
    pub accounts: Vec<DocumentRef>,
    pub admission_complete: bool,
    pub pending_activity_count: u64,
    pub collection_complete: bool,
    pub body_complete: bool,
    pub publication_verified: bool,
    /// Verified activity/source pages, independent of pending index/root publication.
    pub published_activity_count: u64,
    /// Collector manifests do not certify external analysis. See reader-status checkpoints.
    pub analysis_complete: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct IndexPage {
    pub contract: String,
    pub entries: Vec<ActivityEntry>,
    pub children: Vec<DocumentRef>,
}
#[derive(Clone, Debug, Serialize)]
pub struct ManifestHead {
    pub scope: String,
    pub window_id: Option<String>,
    pub local_date: String,
    pub timezone: String,
    pub manifest_id: String,
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct ReaderTick {
    pub admitted: usize,
    pub processed: usize,
    pub pending: u64,
    pub errors: usize,
    pub paused: bool,
}

pub(crate) fn document(manifest: &Manifest, id: String, content: String) -> Record {
    Record {
        schema_version: 1,
        id,
        title: format!(
            "{} {} reader corpus",
            manifest.local_date, manifest.timezone
        ),
        captured_at: manifest.utc_end.to_string(),
        original_urls: vec![],
        provenance: Provenance {
            provider: "archive".into(),
            extractor: CONTRACT.into(),
            retrieved_at: manifest.utc_end.to_string(),
            completeness: if manifest.body_complete {
                crate::sources::FetchStatus::Complete
            } else {
                crate::sources::FetchStatus::Partial
            },
            original_bytes_sha256: None,
        },
        data: RecordData::DailyManifest {
            local_date: manifest.local_date.clone(),
            timezone: manifest.timezone.clone(),
            start_utc: manifest.utc_start.to_string(),
            end_utc: manifest.utc_end.to_string(),
            account_ids: manifest
                .windows
                .iter()
                .map(|w| w.account_id.clone())
                .collect(),
            records: vec![],
            coverage: Coverage {
                complete: manifest.collection_complete,
                admitted: manifest.counts.values().sum(),
                published: manifest.published_activity_count,
                missing_record_ids: vec![],
                gaps: vec![
                    "Reading and L1-L5 analysis require separate external checkpoints.".into(),
                ],
            },
        },
        content,
    }
}

/// Separate runtime seam. Missing Notion/SOP configuration never stops intake.
pub(crate) async fn serve(
    store: Store,
    config: std::sync::Arc<tokio::sync::RwLock<crate::config::ConfigManager>>,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<()> {
    while !cancel.is_cancelled() {
        let snapshot = config.read().await.snapshot();
        if store.reader_tick(&snapshot, 1).is_err() {
            crate::operator::log(
                "reader_manifest_error",
                None,
                serde_json::json!({"reason":"inspect_reader_status"}),
            );
        }
        tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(std::time::Duration::from_secs(1))=>{}}
    }
    Ok(())
}
