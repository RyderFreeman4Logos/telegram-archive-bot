use super::*;
use anyhow::Context;
use std::{collections::BTreeSet, io::Write};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortableExport {
    pub contract: String,
    pub manifest: Manifest,
    pub manifest_id: String,
    pub activities: Vec<ActivityEntry>,
    pub sources: BTreeMap<String, crate::sources::Content>,
    pub documents: Vec<Record>,
    pub checkpoints: Vec<Record>,
    pub limitations: Vec<String>,
}
struct Walk<'a> {
    store: &'a Store,
    config: &'a Config,
    seen: BTreeSet<String>,
    bytes: usize,
    documents: BTreeMap<String, Record>,
    activities: BTreeMap<String, ActivityEntry>,
    // Preserve every revision in the selected immutable closure before event deduplication.
    revisions: BTreeMap<String, SourceRef>,
}
impl Walk<'_> {
    fn visit(&mut self, id: &str) -> Result<()> {
        if !self.seen.insert(id.into()) {
            return Ok(());
        }
        ensure!(
            self.seen.len() <= self.config.limits.semantic_files,
            "reader export document bound exceeded"
        );
        let conn = self.store.connection()?;
        let record = super::store::read_document(
            &conn,
            id,
            self.config.reader.export_bytes.saturating_sub(self.bytes),
        )?;
        ensure!(
            record.id == digest(&record.content),
            "reader document content identity mismatch"
        );
        ensure!(
            record.plan(&self.config.notion)?.publication_key == id,
            "reader document publication identity mismatch"
        );
        self.bytes = self
            .bytes
            .checked_add(serde_json::to_vec(&record)?.len())
            .context("reader export size overflow")?;
        ensure!(
            self.bytes <= self.config.reader.export_bytes,
            "reader export byte bound exceeded"
        );
        let value: Value = serde_json::from_str(&record.content)?;
        let refs = if value.get("scope").is_some() {
            let m: Manifest = serde_json::from_value(value)?;
            ensure!(m.contract == CONTRACT, "invalid reader manifest contract");
            m.index.into_iter().chain(m.accounts).collect::<Vec<_>>()
        } else {
            let page: IndexPage = serde_json::from_value(value)?;
            ensure!(page.contract == CONTRACT, "invalid reader index contract");
            for entry in page.entries {
                for source in &entry.sources {
                    if let Some(revision) = &source.revision_id {
                        if let Some(old) = self.revisions.get(revision) {
                            ensure!(
                                old.source_id == source.source_id
                                    && old.text_sha256 == source.text_sha256,
                                "conflicting reader pinned source reference"
                            );
                        } else {
                            self.revisions.insert(revision.clone(), source.clone());
                        }
                    }
                }
                if let Some(old) = self.activities.get_mut(&entry.activity_id) {
                    for window in &entry.window_ids {
                        if !old.window_ids.contains(window) {
                            old.window_ids.push(window.clone());
                        }
                    }
                    old.window_ids.sort();
                    // All immutable variants remain in documents; the index admits the event once.
                    if entry.observation.retrieved_at > old.observation.retrieved_at {
                        let windows = old.window_ids.clone();
                        *old = entry;
                        old.window_ids = windows;
                    }
                } else {
                    self.activities.insert(entry.activity_id.clone(), entry);
                }
            }
            page.children
        };
        self.documents.insert(id.into(), record);
        drop(conn);
        for link in refs {
            self.visit(&link.document_id)?;
        }
        Ok(())
    }
}
impl Store {
    /// Read only the selected immutable manifest closure. Never export config snapshots, tokens,
    /// unrelated sources, jobs, chat state or operational publication attempts.
    pub fn reader_export(&self, config: &Config, id: &str) -> Result<PortableExport> {
        config.reader.validate()?;
        self.blob_path(id)?;
        let conn = self.connection()?;
        let manifest = super::store::manifest(&conn, id, config.reader.export_bytes)?;
        drop(conn);
        let mut walk = Walk {
            store: self,
            config,
            seen: BTreeSet::new(),
            bytes: 0,
            documents: BTreeMap::new(),
            activities: BTreeMap::new(),
            revisions: BTreeMap::new(),
        };
        walk.visit(id)?;
        let mut sources = BTreeMap::new();
        for source in walk.revisions.values() {
            if let Some(revision) = &source.revision_id {
                if sources.contains_key(revision) {
                    continue;
                }
                let conn = self.connection()?;
                let size:i64=conn.query_row("SELECT length(CAST(body AS BLOB))+length(CAST(metadata AS BLOB)) FROM source_revisions WHERE id=?1 AND source_id=?2",rusqlite::params![revision,source.source_id],|r|r.get(0))?;
                ensure!(
                    size >= 0
                        && size as usize <= config.reader.export_bytes.saturating_sub(walk.bytes),
                    "reader source export bound exceeded"
                );
                let (body, metadata): (String, String) = conn.query_row(
                    "SELECT body,metadata FROM source_revisions WHERE id=?1 AND source_id=?2",
                    rusqlite::params![revision, source.source_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                ensure!(
                    Some(digest(&body)) == source.text_sha256,
                    "reader pinned text digest mismatch"
                );
                let content = crate::sources::Content {
                    body,
                    metadata: serde_json::from_str(&metadata)?,
                };
                walk.bytes += serde_json::to_vec(&content)?.len();
                ensure!(
                    walk.bytes <= config.reader.export_bytes,
                    "reader source export bound exceeded"
                );
                sources.insert(revision.clone(), content);
            }
        }
        let mut checkpoints = BTreeMap::new();
        for document in walk.documents.values() {
            if serde_json::from_str::<Value>(&document.content)?
                .get("scope")
                .is_none()
            {
                continue;
            }
            let key = document.plan(&config.notion)?.publication_key;
            let conn = self.connection()?;
            let rows=conn.prepare("SELECT id,length(CAST(record AS BLOB)) FROM notion_documents WHERE json_extract(record,'$.data.kind')='checkpoint' AND json_extract(record,'$.data.manifest_id')=?1 ORDER BY id LIMIT ?2")?.query_map(rusqlite::params![key,config.limits.semantic_files+1],|r|Ok((r.get::<_,String>(0)?,r.get::<_,usize>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            ensure!(
                rows.len() <= config.limits.semantic_files,
                "reader checkpoint count bound exceeded"
            );
            for (id, size) in rows {
                ensure!(
                    size <= config.reader.export_bytes.saturating_sub(walk.bytes),
                    "reader checkpoint export bound exceeded"
                );
                let record = super::store::read_document(&conn, &id, size)?;
                walk.bytes += size;
                checkpoints.insert(id, record);
            }
        }
        let export=PortableExport {contract:CONTRACT.into(),manifest,manifest_id:id.into(),activities:walk.activities.into_values().collect(),sources,documents:walk.documents.into_values().collect(),checkpoints:checkpoints.into_values().collect(),limitations:vec!["Binary attachment bytes are not embedded; attachment/original/landing URLs and verified-byte states remain explicit.".into(),"Publication readback, human reading and L1-L5 output completion are independent contracts.".into()]};
        ensure!(
            serde_json::to_vec(&export)?.len() <= config.reader.export_bytes,
            "reader final export bound exceeded"
        );
        Ok(export)
    }
}
/// Buffer only a bounded complete representation; no caller can publish a partial render.
struct ExportWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl ExportWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
    fn finish(self) -> Result<String> {
        Ok(String::from_utf8(self.bytes)?)
    }
}
impl Write for ExportWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "reader rendered export bound exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl PortableExport {
    /// Every selected immutable index reference, before canonical activity deduplication.
    /// Distinct observations of the same revision retain their own availability evidence.
    pub fn selected_sources(&self) -> Result<Vec<SourceRef>> {
        let mut sources = Vec::new();
        for document in &self.documents {
            let value: Value = serde_json::from_str(&document.content)?;
            if value.get("scope").is_none() {
                let page: IndexPage = serde_json::from_value(value)?;
                ensure!(page.contract == CONTRACT, "invalid reader index contract");
                sources.extend(page.entries.into_iter().flat_map(|entry| entry.sources));
            }
        }
        Ok(sources)
    }
    /// Actual pretty JSON bytes, including the CLI's terminating newline, within the explicit cap.
    pub fn render_json(&self, max_bytes: usize) -> Result<String> {
        let mut out = ExportWriter::new(max_bytes);
        serde_json::to_writer_pretty(&mut out, self)?;
        out.write_all(b"\n")?;
        out.finish()
    }
    /// Actual Markdown bytes, including the embedded corpus and full source text, within the cap.
    pub fn markdown(&self, max_bytes: usize) -> Result<String> {
        let mut out = ExportWriter::new(max_bytes);
        write!(
            &mut out,
            "# Daily corpus {} ({})\n\nUTC: [{}, {})\n\nSOP: {} {}\n\n",
            self.manifest.local_date,
            self.manifest.timezone,
            self.manifest.utc_start,
            self.manifest.utc_end,
            self.manifest.sop.id,
            self.manifest.sop.version
        )?;
        out.write_all(b"## Exhaustive index, manifests and checkpoints\n\n```json\n")?;
        serde_json::to_writer_pretty(&mut out, self)?;
        out.write_all(b"\n```\n\n")?;
        for (id, content) in &self.sources {
            write!(
                &mut out,
                "## Captured revision {id}\n\n{}\n\n",
                content.body
            )?;
        }
        out.finish()
    }
}
