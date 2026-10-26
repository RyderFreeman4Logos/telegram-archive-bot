//! DA05 (#72): versioned portable records and a bounded, explicitly authorized ntn adapter.
//! DA06 owns durable queueing/reconciliation; no implicit write retries occur here.
use crate::{
    config::{Notion, NotionDestination},
    sources::FetchStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;

pub const SCHEMA_VERSION: u32 = 1;
pub const NTN_VERSION: &str = "ntn 0.22.10";
pub const API_VERSION: &str = "2026-03-11";
pub const CANONICAL_VERSION: &str = "notion-ordered-text-v1";
pub const JSON_CANONICAL_VERSION: &str = "notion-ordered-text-json-v2";
const TEXT_HEADING: &str = "Captured text (managed)";
const JSON_TEXT_HEADING: &str = "Captured text (managed; JSON string encoding)";

#[cfg(feature = "ntn")]
#[path = "notion_client.rs"]
mod client;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub schema_version: u32,
    pub id: String,
    pub title: String,
    pub captured_at: String,
    pub original_urls: Vec<String>,
    pub provenance: Provenance,
    pub data: RecordData,
    pub content: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    pub provider: String,
    pub extractor: String,
    pub retrieved_at: String,
    pub completeness: FetchStatus,
    pub original_bytes_sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordData {
    Account {
        observation: Da01Observation,
    },
    Source {
        provider: String,
        external_id: String,
    },
    Revision {
        source_id: String,
        revision_id: String,
        authored_at: Option<String>,
    },
    Activity {
        observation: Da01Observation,
    },
    Relation {
        source_id: String,
        target_source_id: String,
        relation: crate::sources::RelationKind,
    },
    DailyManifest {
        local_date: String,
        timezone: String,
        start_utc: String,
        end_utc: String,
        account_ids: Vec<String>,
        records: Vec<RecordLink>,
        coverage: Coverage,
    },
    AgentOutput {
        manifest_id: String,
        sop_version: String,
        level: u8,
        sources: Vec<RecordLink>,
    },
    Checkpoint {
        manifest_id: String,
        sop_version: String,
        completed_levels: Vec<u8>,
        remaining_record_ids: Vec<String>,
    },
}

/// Lossless DA01 wire envelope constructed from the actual
/// AccountObservation/ActivityObservation types, not invented timestamps.
/// Keep Unknown/Instant/Interval, precision, provenance and unresolved IDs intact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Da01Observation {
    pub schema_version: u32,
    pub value: serde_json::Value,
}

impl Da01Observation {
    /// Preserve the complete account observation, including unresolved identity.
    pub fn from_account(
        observation: &crate::activities::AccountObservation,
    ) -> Result<Self, PublicationError> {
        Ok(Self {
            schema_version: 1,
            value: serde_json::to_value(observation).map_err(|_| Failure::InvalidPayload)?,
        })
    }

    /// Preserve the observation's own version, evidence and immutable source/view pins.
    pub fn from_activity(
        observation: &crate::activities::ActivityObservation,
    ) -> Result<Self, PublicationError> {
        Ok(Self {
            schema_version: observation.schema_version,
            value: serde_json::to_value(observation).map_err(|_| Failure::InvalidPayload)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordLink {
    pub record_id: String,
    pub source_id: String,
    pub revision_id: Option<String>,
    pub original_url: String,
    pub notion_page_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Coverage {
    pub complete: bool,
    pub admitted: u64,
    pub published: u64,
    pub missing_record_ids: Vec<String>,
    pub gaps: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessState {
    Disabled,
    CompiledOut,
    PendingConfiguration,
    Ready,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Prerequisite {
    Executable,
    TokenReference,
    TokenUnavailable,
    Destination,
    InvalidDestination,
    Bounds,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Readiness {
    pub state: ReadinessState,
    pub missing: Vec<Prerequisite>,
    pub network_probes_performed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Failure {
    Disabled,
    CompiledOut,
    PendingConfiguration,
    UnsupportedTool,
    BlockedAuth,
    RetryWait,
    Conflict,
    Reconcile,
    Cancelled,
    Limit,
    InvalidPayload,
    InvalidOutput,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationError {
    pub state: Failure,
    pub page_id: Option<String>,
    pub retry_after_seconds: Option<u64>,
}
impl std::fmt::Display for PublicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Notion publication: {:?}", self.state)
    }
}
impl std::error::Error for PublicationError {}
impl From<Failure> for PublicationError {
    fn from(state: Failure) -> Self {
        Self {
            state,
            page_id: None,
            retry_after_seconds: None,
        }
    }
}

// Local structured stderr/journal only. Callers supply fixed tags and counts,
// never provider text, identifiers, arguments, paths, or ToolReport contents.
fn log_limit(reason: &'static str, scope: &'static str, counts: &[(&'static str, u64)]) {
    let mut detail = json!({"reason":reason,"scope":scope});
    for (name, value) in counts {
        detail[*name] = json!(value);
    }
    crate::operator::log("notion_limit", None, detail);
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedPage {
    pub page_id: String,
    pub publication_key: String,
    pub canonical_digest: String,
    pub content_sha256: String,
    pub schema_version: u32,
    pub destination_key: String,
    #[serde(default)]
    pub block_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationProgress {
    pub page_id: String,
    pub written_blocks: usize,
    pub verified_blocks: usize,
}
pub type ProgressObserver<'a> =
    dyn Fn(&PublicationProgress) -> Result<(), PublicationError> + Send + Sync + 'a;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedBlock {
    pub kind: String,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationPlan {
    pub publication_key: String,
    pub page_title: String,
    pub canonical_version: String,
    pub canonical_digest: String,
    pub content_sha256: String,
    pub managed_blocks: Vec<ManagedBlock>,
}

impl RecordData {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Account { .. } => "account",
            Self::Source { .. } => "source",
            Self::Revision { .. } => "revision",
            Self::Activity { .. } => "activity",
            Self::Relation { .. } => "relation",
            Self::DailyManifest { .. } => "daily_manifest",
            Self::AgentOutput { .. } => "agent_output",
            Self::Checkpoint { .. } => "checkpoint",
        }
    }
}

impl Record {
    /// Deterministic planning without credentials or ntn; refused bounds emit a local count-only log.
    pub fn plan(&self, config: &Notion) -> Result<PublicationPlan, PublicationError> {
        if self.schema_version != SCHEMA_VERSION
            || self.id.is_empty()
            || self.id.len() > 512
            || self.title.is_empty()
            || self.captured_at.is_empty()
            || self.provenance.provider.is_empty()
            || self.provenance.extractor.is_empty()
            || self.provenance.retrieved_at.is_empty()
            || self.original_urls.iter().any(|url| !public_link(url))
            || self
                .provenance
                .original_bytes_sha256
                .as_ref()
                .is_some_and(|hash| {
                    hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit())
                })
            || matches!(&self.data, RecordData::Revision { source_id, revision_id, .. } if source_id.is_empty() || revision_id != &self.id)
            || matches!(&self.data, RecordData::AgentOutput { level, .. } if !(1..=5).contains(level))
            || matches!(&self.data, RecordData::Checkpoint { completed_levels, .. } if completed_levels.iter().any(|level| !(1..=5).contains(level)))
            || matches!(&self.data, RecordData::Account { observation } | RecordData::Activity { observation } if observation.schema_version != 1 || !observation.value.is_object())
        {
            return Err(Failure::InvalidPayload.into());
        }
        config.validate().map_err(|_| Failure::InvalidPayload)?;
        let serialized = serde_json::to_vec(self).map_err(|_| Failure::InvalidPayload)?;
        if serialized.len() > config.record_bytes {
            log_limit(
                "payload",
                "record_bytes",
                &[
                    ("bytes", serialized.len() as u64),
                    ("limit", config.record_bytes as u64),
                ],
            );
            return Err(Failure::Limit.into());
        }
        let publication_key = crate::digest(
            serde_json::to_vec(&json!([SCHEMA_VERSION, self.data.kind(), self.id]))
                .map_err(|_| Failure::InvalidPayload)?,
        );
        let content_sha256 = crate::digest(&self.content);
        // S87: Notion accepted a PDF's text but removed its U+000C on readback.
        // Keep ordinary v1 plans byte-identical. Encode lossy controls and the
        // DA05/#93 Unicode separators reversibly, including metadata literals.
        let json_content = self.content.chars().any(unsafe_control)
            || std::str::from_utf8(&serialized)
                .map_err(|_| Failure::InvalidPayload)?
                .chars()
                .any(unsafe_control);
        let canonical_version = if json_content {
            JSON_CANONICAL_VERSION
        } else {
            CANONICAL_VERSION
        };
        let encoded_content = if json_content {
            Some(escape_json_controls(
                serde_json::to_string(&self.content).map_err(|_| Failure::InvalidPayload)?,
                config.record_bytes,
            )?)
        } else {
            None
        };
        let mut metadata = serde_json::to_value(self).map_err(|_| Failure::InvalidPayload)?;
        metadata
            .as_object_mut()
            .ok_or(Failure::InvalidPayload)?
            .remove("content");
        metadata["content_utf8_sha256"] = json!(content_sha256);
        metadata["canonical_version"] = json!(canonical_version);
        if json_content {
            metadata["content_encoding"] = json!("json-string-v1");
        }
        let metadata =
            serde_json::to_string_pretty(&metadata).map_err(|_| Failure::InvalidPayload)?;
        let metadata = if let Some(content) = &encoded_content {
            // Bound the entire expanded representation before request construction.
            escape_json_controls(metadata, config.record_bytes - content.len())?
        } else {
            metadata
        };
        let mut blocks = vec![
            block("paragraph", format!("ARCHIVE_RECORD_V1 {publication_key}")),
            block("heading_2", "Record metadata (managed)"),
        ];
        blocks.extend(
            chunks(&metadata)
                .into_iter()
                .map(|text| block("paragraph", text)),
        );
        blocks.push(block(
            "heading_2",
            if json_content {
                JSON_TEXT_HEADING
            } else {
                TEXT_HEADING
            },
        ));
        blocks.extend(
            chunks(encoded_content.as_deref().unwrap_or(&self.content))
                .into_iter()
                .map(|text| block("paragraph", text)),
        );
        blocks.push(block(
            "paragraph",
            format!("END_ARCHIVE_RECORD_V1 {publication_key}"),
        ));
        blocks.push(block(
            "heading_2",
            "Human annotations and downstream analysis (unmanaged)",
        ));
        if blocks.len() > config.max_blocks {
            log_limit(
                "payload",
                "record_blocks",
                &[
                    ("blocks", blocks.len() as u64),
                    ("limit", config.max_blocks as u64),
                ],
            );
            return Err(Failure::Limit.into());
        }
        let canonical_digest =
            crate::digest(serde_json::to_vec(&blocks).map_err(|_| Failure::InvalidPayload)?);
        Ok(PublicationPlan {
            page_title: format!("archive-v1/{}/{publication_key}", self.data.kind()),
            publication_key,
            canonical_version: canonical_version.into(),
            canonical_digest,
            content_sha256,
            managed_blocks: blocks,
        })
    }
}

impl PublicationPlan {
    /// Decode the actual returned body after its ordered blocks were checked.
    /// An unchanged hash printed in metadata is never content verification.
    #[cfg(feature = "ntn")]
    fn verify_returned_content(&self, blocks: &[ManagedBlock]) -> Result<(), PublicationError> {
        let heading = match self.canonical_version.as_str() {
            CANONICAL_VERSION => TEXT_HEADING,
            JSON_CANONICAL_VERSION => JSON_TEXT_HEADING,
            _ => return Err(Failure::Conflict.into()),
        };
        let start = blocks
            .iter()
            .position(|b| b.kind == "heading_2" && b.text == heading)
            .ok_or(Failure::Conflict)?
            + 1;
        let end = blocks.len().checked_sub(2).ok_or(Failure::Conflict)?;
        let body = blocks.get(start..end).ok_or(Failure::Conflict)?;
        if body.iter().any(|b| b.kind != "paragraph") {
            return Err(Failure::Conflict.into());
        }
        let text: String = body.iter().map(|b| b.text.as_str()).collect();
        let decoded = if self.canonical_version == JSON_CANONICAL_VERSION {
            serde_json::from_str::<String>(&text).map_err(|_| Failure::Conflict)?
        } else {
            text
        };
        if crate::digest(decoded) != self.content_sha256 {
            return Err(Failure::Conflict.into());
        }
        Ok(())
    }
}

fn unsafe_control(ch: char) -> bool {
    (ch.is_control() && ch != '\n' && ch != '\t') || matches!(ch, '\u{2028}' | '\u{2029}')
}

// Input is serialized JSON; escaping its remaining unsafe literal characters keeps
// decoded strings identical. Pretty-print whitespace (LF/tab) remains readable.
fn escape_json_controls(value: String, limit: usize) -> Result<String, PublicationError> {
    let bytes = value
        .chars()
        .try_fold(0usize, |bytes, ch| {
            bytes.checked_add(if unsafe_control(ch) { 6 } else { ch.len_utf8() })
        })
        .ok_or_else(|| {
            log_limit(
                "payload",
                "encoded_bytes_overflow",
                &[("limit", limit as u64)],
            );
            Failure::Limit
        })?;
    if bytes > limit {
        log_limit(
            "payload",
            "encoded_bytes",
            &[("bytes", bytes as u64), ("limit", limit as u64)],
        );
        return Err(Failure::Limit.into());
    }
    if bytes == value.len() {
        return Ok(value);
    }
    use std::fmt::Write;
    let mut escaped = String::with_capacity(bytes);
    for ch in value.chars() {
        if unsafe_control(ch) {
            write!(&mut escaped, "\\u{:04x}", u32::from(ch))
                .map_err(|_| Failure::InvalidPayload)?;
        } else {
            escaped.push(ch);
        }
    }
    Ok(escaped)
}

fn block(kind: &str, text: impl Into<String>) -> ManagedBlock {
    ManagedBlock {
        kind: kind.into(),
        text: text.into(),
    }
}

fn public_link(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
    })
}

// UTF-16 units are conservative for Notion's 2000-character limit, including astral text.
// No Unicode normalization, newline insertion, control stripping or truncation is performed.
fn chunks(value: &str) -> Vec<&str> {
    let (mut start, mut units) = (0, 0);
    let mut result = vec![];
    for (offset, ch) in value.char_indices() {
        if units + ch.len_utf16() > 2000 {
            result.push(&value[start..offset]);
            start = offset;
            units = 0;
        }
        units += ch.len_utf16();
    }
    result.push(&value[start..]);
    result
}

pub(crate) fn notion_id(value: &str) -> Option<String> {
    if !(value.len() == 32
        || (value.len() == 36
            && [8, 13, 18, 23]
                .into_iter()
                .all(|i| value.as_bytes()[i] == b'-')))
    {
        return None;
    }
    let compact = value.replace('-', "").to_ascii_lowercase();
    if compact.len() != 32 || !compact.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!(
        "{}-{}-{}-{}-{}",
        &compact[..8],
        &compact[8..12],
        &compact[12..16],
        &compact[16..20],
        &compact[20..]
    ))
}

impl NotionDestination {
    pub fn identity(&self) -> Option<String> {
        let value = match self {
            Self::Page { id } => json!({"page_id": notion_id(id)?}),
            Self::DataSource { id, title_property } => {
                if title_property.is_empty() || title_property.encode_utf16().count() > 2000 {
                    return None;
                }
                json!({"data_source_id": notion_id(id)?, "title_property": title_property})
            }
        };
        Some(crate::digest(serde_json::to_vec(&value).ok()?))
    }
}

/// Local prerequisites only. Remote authorization is established by actual requests, not this report.
/// Disabled/compiled-out return before filesystem or secret access, including executable probes.
pub fn readiness(config: &Notion) -> Readiness {
    if !config.enabled {
        return Readiness::state(ReadinessState::Disabled);
    }
    if !cfg!(feature = "ntn") {
        return Readiness::state(ReadinessState::CompiledOut);
    }
    use std::os::unix::fs::PermissionsExt;
    let mut missing = vec![];
    if config.validate().is_err() {
        missing.push(Prerequisite::Bounds);
    }
    if !config.executable.as_ref().is_some_and(|p| {
        p.is_absolute()
            && p.metadata()
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }) {
        missing.push(Prerequisite::Executable);
    }
    match &config.token {
        None => missing.push(Prerequisite::TokenReference),
        Some(token) if !token.read().is_ok_and(|value| value.len() <= 16384) => {
            missing.push(Prerequisite::TokenUnavailable)
        }
        Some(_) => {}
    }
    match &config.destination {
        None => missing.push(Prerequisite::Destination),
        Some(destination) if destination.identity().is_none() => {
            missing.push(Prerequisite::InvalidDestination)
        }
        Some(_) => {}
    }
    Readiness {
        state: if missing.is_empty() {
            ReadinessState::Ready
        } else {
            ReadinessState::PendingConfiguration
        },
        missing,
        network_probes_performed: false,
    }
}

impl Readiness {
    fn state(state: ReadinessState) -> Self {
        Self {
            state,
            missing: vec![],
            network_probes_performed: false,
        }
    }
}

pub struct Adapter {
    config: Notion,
    work_root: PathBuf,
}
impl Adapter {
    pub fn new(config: &Notion, work_root: &Path) -> Self {
        Self {
            config: config.clone(),
            work_root: work_root.to_owned(),
        }
    }
    fn admit(&self) -> Result<(), PublicationError> {
        match readiness(&self.config).state {
            ReadinessState::Disabled => Err(Failure::Disabled.into()),
            ReadinessState::CompiledOut => Err(Failure::CompiledOut.into()),
            ReadinessState::PendingConfiguration => Err(Failure::PendingConfiguration.into()),
            ReadinessState::Ready => Ok(()),
        }
    }
    pub async fn create(
        &self,
        record: &Record,
        cancel: &CancellationToken,
    ) -> Result<VerifiedPage, PublicationError> {
        self.create_observed(record, cancel, &|_| Ok(())).await
    }
    pub async fn create_observed(
        &self,
        record: &Record,
        cancel: &CancellationToken,
        observer: &ProgressObserver<'_>,
    ) -> Result<VerifiedPage, PublicationError> {
        self.admit()?;
        #[cfg(feature = "ntn")]
        {
            client::create(self, record, cancel, observer).await
        }
        #[cfg(not(feature = "ntn"))]
        {
            let _ = (record, cancel, observer, &self.work_root);
            Err(Failure::CompiledOut.into())
        }
    }
    pub async fn verify(
        &self,
        page_id: &str,
        record: &Record,
        cancel: &CancellationToken,
    ) -> Result<VerifiedPage, PublicationError> {
        self.reconcile(Some(page_id), record, cancel, &|_| Ok(()))
            .await?
            .ok_or_else(|| Failure::InvalidOutput.into())
    }
    /// Read-only lookup/verification. An empty marker search NEVER authorizes creation.
    pub async fn reconcile(
        &self,
        page_id: Option<&str>,
        record: &Record,
        cancel: &CancellationToken,
        observer: &ProgressObserver<'_>,
    ) -> Result<Option<VerifiedPage>, PublicationError> {
        self.admit()?;
        #[cfg(feature = "ntn")]
        {
            if let Some(page_id) = page_id {
                client::verify(self, page_id, record, cancel, observer)
                    .await
                    .map(Some)
            } else {
                client::find(self, record, cancel, observer).await
            }
        }
        #[cfg(not(feature = "ntn"))]
        {
            let _ = (page_id, record, cancel, observer);
            Err(Failure::CompiledOut.into())
        }
    }
}
