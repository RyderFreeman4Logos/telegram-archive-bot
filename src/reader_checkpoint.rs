//! External assertions are versioned evidence, not a model pipeline or a remote read verifier.
use super::*;
use anyhow::Context;
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadState {
    FullyRead,
    PartiallyRead,
    MetadataOnly,
    Inaccessible,
    OutOfScope,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    CapturedText,
    VerifiedBytes,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadEvidence {
    pub kind: EvidenceKind,
    pub sha256: String,
    pub reader: String,
    pub method: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRead {
    pub source_id: String,
    pub revision_id: Option<String>,
    pub state: ReadState,
    pub reason: String,
    pub evidence: Option<ReadEvidence>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayerState {
    InProgress,
    Blocked,
    Complete,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputRef {
    pub page_id: String,
    pub sha256: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReaderProvenance {
    pub worker: String,
    pub model: Option<String>,
    pub version: String,
    pub method: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayerCheckpoint {
    pub contract: String,
    pub session_id: String,
    pub layer: u8,
    pub specification_version: String,
    pub manifest_id: String,
    pub input_manifest_sha256: String,
    pub local_date: String,
    pub timezone: String,
    pub utc_start: i64,
    pub utc_end: i64,
    pub sop: ReaderSop,
    pub state: LayerState,
    pub outputs: Vec<OutputRef>,
    pub cursor: Option<Value>,
    pub processed_activity_ids: Vec<String>,
    pub readings: Vec<SourceRead>,
    pub unresolved_sources: Vec<String>,
    pub provenance: ReaderProvenance,
}
#[derive(Clone, Debug, Serialize)]
pub struct SavedCheckpoint {
    pub id: String,
    pub manifest_id: String,
    pub layer: u8,
    pub stale: bool,
    pub state: LayerState,
}
fn hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn text(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 4096
}

impl Store {
    /// Import an external reader's explicit assertion against one immutable corpus snapshot.
    /// A changed current head requires deliberate --allow-stale, never a relative-date reparse.
    pub fn record_reader_checkpoint(
        &self,
        config: &Config,
        value: &LayerCheckpoint,
        allow_stale: bool,
    ) -> Result<SavedCheckpoint> {
        ensure!(
            value.contract == CONTRACT
                && (1..=5).contains(&value.layer)
                && text(&value.session_id)
                && text(&value.specification_version),
            "invalid reader layer contract"
        );
        ensure!(
            text(&value.provenance.worker)
                && text(&value.provenance.version)
                && text(&value.provenance.method)
                && value.provenance.model.as_ref().is_none_or(|s| text(s)),
            "reader provenance is required"
        );
        let raw = serde_json::to_string(value)?;
        ensure!(
            raw.len() <= config.notion.record_bytes.min(config.reader.export_bytes),
            "reader checkpoint exceeds bound"
        );
        let export = self.reader_export(config, &value.manifest_id)?;
        let m = &export.manifest;
        let conn = self.connection()?;
        let root =
            super::store::read_document(&conn, &value.manifest_id, config.notion.record_bytes)?;
        drop(conn);
        ensure!(
            root.id == value.input_manifest_sha256,
            "reader input manifest digest mismatch"
        );
        ensure!(
            value.local_date == m.local_date
                && value.timezone == m.timezone
                && value.utc_start == m.utc_start
                && value.utc_end == m.utc_end
                && value.sop == m.sop,
            "reader absolute window or SOP mismatch"
        );
        let admitted: BTreeSet<_> = export
            .activities
            .iter()
            .map(|a| a.activity_id.as_str())
            .collect();
        let processed: BTreeSet<_> = value
            .processed_activity_ids
            .iter()
            .map(String::as_str)
            .collect();
        ensure!(
            processed.len() == value.processed_activity_ids.len() && processed.is_subset(&admitted),
            "reader processed IDs are duplicated or outside the manifest"
        );
        let unresolved: BTreeSet<_> = value
            .unresolved_sources
            .iter()
            .map(String::as_str)
            .collect();
        ensure!(
            unresolved.len() == value.unresolved_sources.len(),
            "duplicate unresolved reader source"
        );
        let selected_sources = export.selected_sources()?;
        let mut sources: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for source in &selected_sources {
            sources
                .entry((source.source_id.as_str(), source.revision_id.as_deref()))
                .or_default()
                .push(source);
        }
        let mut read = BTreeSet::new();
        let mut gaps = BTreeSet::new();
        for assessment in &value.readings {
            let key = (
                assessment.source_id.as_str(),
                assessment.revision_id.as_deref(),
            );
            let variants = sources
                .get(&key)
                .context("reader assessment is outside pinned sources")?;
            ensure!(read.insert(key), "duplicate reader source assessment");
            if assessment.state == ReadState::FullyRead {
                let evidence = assessment.evidence.as_ref().context(
                    "fully read requires complete-content evidence, not metadata or filename",
                )?;
                ensure!(
                    hash(&evidence.sha256) && text(&evidence.reader) && text(&evidence.method),
                    "invalid complete-content read evidence"
                );
                let proven = variants.iter().any(|source| match evidence.kind {
                    EvidenceKind::CapturedText => {
                        source.capture_status == crate::sources::FetchStatus::Complete
                            && source.revision_id.is_some()
                            && source.text_sha256.as_deref() == Some(evidence.sha256.as_str())
                            && source
                                .revision_id
                                .as_ref()
                                .and_then(|id| export.sources.get(id))
                                .is_some_and(|c| !c.body.is_empty())
                    }
                    EvidenceKind::VerifiedBytes => source.enrichment.as_ref().is_some_and(|e| {
                        e.attachments.iter().any(|a| {
                            a.attachment_state == "verified"
                                && a.verified_sha256.as_deref() == Some(evidence.sha256.as_str())
                        })
                    }),
                });
                ensure!(
                    proven,
                    "read digest is not the pinned complete captured text or verified attachment bytes"
                );
            } else {
                ensure!(
                    text(&assessment.reason),
                    "non-full reader state requires an explicit reason"
                );
                if assessment.state != ReadState::OutOfScope {
                    gaps.insert(assessment.source_id.as_str());
                }
            }
        }
        ensure!(
            unresolved == gaps,
            "unresolved source accounting must equal partial/metadata/inaccessible assessments"
        );
        if value.state == LayerState::Complete {
            ensure!(
                m.sop_configured
                    && !value.outputs.is_empty()
                    && value.cursor.is_none()
                    && processed == admitted
                    && read.len() == sources.len(),
                "complete layer requires configured SOP, outputs, exhausted cursor and exhaustive activity/source coverage"
            );
        }
        ensure!(
            value.outputs.len() <= 1000
                && value
                    .outputs
                    .iter()
                    .all(|o| text(&o.page_id) && hash(&o.sha256)),
            "invalid reader output references"
        );
        let id = digest(&raw);
        let mut record = super::document(m, id, raw);
        record.title = format!(
            "{} {} L{} reader checkpoint",
            m.local_date, m.timezone, value.layer
        );
        record.data = RecordData::Checkpoint {
            manifest_id: value.manifest_id.clone(),
            sop_version: value.sop.version.clone(),
            completed_levels: if value.state == LayerState::Complete {
                vec![value.layer]
            } else {
                vec![]
            },
            remaining_record_ids: admitted
                .difference(&processed)
                .map(|id| (*id).to_owned())
                .collect(),
        };
        record.provenance.extractor = "external-reader-attestation-v1".into();
        record.plan(&config.notion)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<String> = tx
            .query_row(
                "SELECT manifest_id FROM reader_heads WHERE scope=?1",
                [&m.scope],
                |r| r.get(0),
            )
            .optional()?;
        let stale = current.as_deref() != Some(&value.manifest_id);
        ensure!(
            !stale || allow_stale,
            "reader input manifest changed; explicitly resume the pinned revision or restart against the current head"
        );
        let key = crate::notion_records::save_document(&tx, &record)?;
        tx.execute(
            "INSERT OR IGNORE INTO reader_checkpoints VALUES(?1,?2,?3,?4,?5)",
            params![key, m.scope, value.manifest_id, value.layer, stale],
        )?;
        tx.commit()?;
        Ok(SavedCheckpoint {
            id: key,
            manifest_id: value.manifest_id.clone(),
            layer: value.layer,
            stale,
            state: value.state,
        })
    }
    /// Currentness and L1-L5 completion are independent of source publication and reading extent.
    pub fn reader_checkpoints(&self, config: &Config, manifest_id: &str) -> Result<Value> {
        let conn = self.connection()?;
        let manifest = super::store::manifest(&conn, manifest_id, config.reader.export_bytes)?;
        let current: Option<String> = conn
            .query_row(
                "SELECT manifest_id FROM reader_heads WHERE scope=?1",
                [&manifest.scope],
                |r| r.get(0),
            )
            .optional()?;
        let stale = current.as_deref() != Some(manifest_id);
        let mut layers = BTreeMap::new();
        let mut complete = !stale;
        let mut reads = BTreeMap::new();
        for layer in 1..=5 {
            let raw:Option<String>=conn.query_row("SELECT d.record FROM notion_documents d WHERE json_extract(d.record,'$.data.kind')='checkpoint' AND json_extract(d.record,'$.data.manifest_id')=?1 AND CAST(json_extract(CASE WHEN json_valid(json_extract(d.record,'$.content')) THEN json_extract(d.record,'$.content') ELSE '{}' END,'$.layer') AS INTEGER)=?2 ORDER BY json_extract(CASE WHEN json_valid(json_extract(d.record,'$.content')) THEN json_extract(d.record,'$.content') ELSE '{}' END,'$.state')='complete' DESC,d.rowid DESC LIMIT 1",params![manifest_id,layer],|r|r.get(0)).optional()?;
            if let Some(raw) = raw {
                ensure!(
                    raw.len() <= config.notion.record_bytes,
                    "reader checkpoint exceeds bound"
                );
                let record: Record = serde_json::from_str(&raw)?;
                let checkpoint: LayerCheckpoint = serde_json::from_str(&record.content)?;
                complete &= checkpoint.state == LayerState::Complete;
                for r in &checkpoint.readings {
                    reads.insert((r.source_id.clone(), r.revision_id.clone()), r.state);
                }
                layers.insert(layer, serde_json::to_value(&checkpoint)?);
            } else {
                complete = false;
            }
        }
        let mut reading_counts = BTreeMap::<String, u64>::new();
        for state in reads.values() {
            *reading_counts
                .entry(serde_json::to_value(state)?.as_str().unwrap().into())
                .or_default() += 1;
        }
        Ok(
            serde_json::json!({"manifest_id":manifest_id,"stale":stale,"layers":layers,"complete_summary":complete,"collection_complete":manifest.collection_complete,"body_complete":manifest.body_complete,"publication_verified":manifest.publication_verified,"reading_counts":reading_counts,"reading_evidence":"external reader assertions checked against pinned captured hashes; no remote read/output audit is implied"}),
        )
    }
}
