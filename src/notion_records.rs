//! Read canonical observations without changing source bodies or revision ownership.
use crate::{
    activities::{AccountObservation, ActivityObservation},
    notion::{Da01Observation, Provenance, Record, RecordData},
    notion_queue::Intent,
    sources::{ContentMetadata, FetchStatus, Identity},
    storage::Store,
};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

fn urls(conn: &rusqlite::Connection, source: &str, identity: &Identity) -> Result<Vec<String>> {
    let mut urls = conn
        .prepare("SELECT url FROM source_aliases WHERE source_id=?1 ORDER BY url")?
        .query_map([source], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let canonical = identity.url();
    if crate::network::source_url(&canonical).is_ok() && !urls.contains(&canonical) {
        urls.push(canonical);
    }
    Ok(urls)
}

impl Store {
    /// Persist an immutable derived document and its queue intent in one transaction.
    pub fn record_notion_document(&self, record: &Record) -> Result<String> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let key = save_document(&tx, record)?;
        tx.commit()?;
        Ok(key)
    }
    pub(crate) fn notion_record(&self, intent: &Intent, max_bytes: usize) -> Result<Record> {
        if let Some(record) = &intent.record {
            ensure!(record.len() <= max_bytes, "Notion record exceeds bound");
            return Ok(serde_json::from_str(record)?);
        }
        let conn = self.connection()?;
        let mut record = Record {
            schema_version: 1,
            id: intent.entity.clone(),
            title: format!("Archived {}", intent.kind),
            captured_at: intent.created_at.to_string(),
            original_urls: vec![],
            provenance: Provenance {
                provider: "archive".into(),
                extractor: "canonical-v1".into(),
                retrieved_at: intent.created_at.to_string(),
                completeness: FetchStatus::Complete,
                original_bytes_sha256: None,
            },
            data: RecordData::Source {
                provider: "archive".into(),
                external_id: intent.entity.clone(),
            },
            content: String::new(),
        };
        match intent.kind.as_str() {
            "revision" | "observation" => {
                let id = intent
                    .observation
                    .ok_or_else(|| anyhow::anyhow!("missing source observation pin"))?;
                let evidence_bytes: i64 = conn.query_row("SELECT length(CAST(evidence AS BLOB))+length(CAST(extractor AS BLOB)) FROM observations WHERE id=?1", [id], |r|r.get(0))?;
                ensure!(
                    evidence_bytes >= 0 && evidence_bytes as usize <= max_bytes,
                    "Notion observation exceeds record bound"
                );
                let (source,revision,time,status,extractor,evidence,raw,provider,external):(String,Option<String>,i64,String,String,String,Option<String>,String,String)=conn.query_row("SELECT o.source_id,o.revision_id,o.observed_at,o.status,o.extractor,o.evidence,o.raw_digest,s.provider,s.external_id FROM observations o JOIN sources s ON s.id=o.source_id WHERE o.id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?)))?;
                let linked_bytes:i64=conn.query_row("SELECT coalesce((SELECT sum(length(CAST(url AS BLOB))) FROM source_aliases WHERE source_id=?1),0)+coalesce((SELECT sum(length(target_id)+length(kind)+32) FROM relations WHERE observation_id=?2),0)",params![source,id],|r|r.get(0))?;
                ensure!(
                    linked_bytes >= 0 && linked_bytes as usize <= max_bytes,
                    "Notion source links exceed record bound"
                );
                record.original_urls = urls(
                    &conn,
                    &source,
                    &Identity {
                        provider: provider.clone(),
                        external_id: external.clone(),
                    },
                )?;
                record.captured_at = time.to_string();
                record.provenance = Provenance {
                    provider: provider.clone(),
                    extractor,
                    retrieved_at: time.to_string(),
                    completeness: serde_json::from_value(json!(status))?,
                    original_bytes_sha256: match raw {
                        Some(raw) => Some(raw),
                        None => conn.query_row("SELECT json_extract(manifest,'$.raw_digest') FROM semantic_unavailable WHERE kind='observation' AND entity_id=?1", [id.to_string()], |r|r.get::<_,Option<String>>(0)).optional()?.flatten(),
                    },
                };
                if intent.kind == "revision" {
                    ensure!(
                        revision.as_deref() == Some(intent.entity.as_str()),
                        "Notion revision observation owner mismatch"
                    );
                    let bytes:i64=conn.query_row("SELECT length(CAST(body AS BLOB))+length(CAST(metadata AS BLOB)) FROM source_revisions WHERE id=?1 AND source_id=?2",params![intent.entity,source],|r|r.get(0))?;
                    ensure!(
                        bytes >= 0 && bytes as usize <= max_bytes,
                        "Notion source exceeds record bound"
                    );
                    let (body, metadata): (String, String) = conn.query_row(
                        "SELECT body,metadata FROM source_revisions WHERE id=?1 AND source_id=?2",
                        params![intent.entity, source],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )?;
                    let metadata: ContentMetadata = serde_json::from_str(&metadata)?;
                    record.title = metadata
                        .title
                        .unwrap_or_else(|| format!("Source revision {}", intent.entity));
                    record.content = body;
                    record.data = RecordData::Revision {
                        source_id: source,
                        revision_id: intent.entity.clone(),
                        authored_at: metadata.authored_at,
                    };
                } else {
                    record.id = format!("{source}/observation/{id}");
                    record.data = RecordData::Source {
                        provider,
                        external_id: external,
                    };
                    let relations=conn.prepare("SELECT target_id,kind FROM relations WHERE observation_id=?1 ORDER BY kind,target_id")?.query_map([id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
                    let metadata = if let Some(revision) = &revision {
                        let bytes: i64 = conn.query_row("SELECT length(CAST(metadata AS BLOB)) FROM source_revisions WHERE id=?1 AND source_id=?2", params![revision,source], |r|r.get(0))?;
                        ensure!(
                            bytes >= 0 && bytes as usize <= max_bytes,
                            "Notion revision metadata exceeds bound"
                        );
                        let value: String = conn.query_row(
                            "SELECT metadata FROM source_revisions WHERE id=?1 AND source_id=?2",
                            params![revision, source],
                            |r| r.get(0),
                        )?;
                        serde_json::from_str::<Value>(&value)?
                    } else {
                        Value::Null
                    };
                    record.content = serde_json::to_string_pretty(
                        &json!({"schema_version":1,"source_id":source,"source_observation_id":id,"revision_id":revision,"revision_metadata":metadata,"observed_at":time,"evidence":serde_json::from_str::<Value>(&evidence)?,"relations":relations}),
                    )?;
                }
            }
            "account" => {
                let value: String = conn.query_row(
                    "SELECT manifest FROM account_observations WHERE id=?1",
                    [&intent.entity],
                    |r| r.get(0),
                )?;
                ensure!(
                    value.len() <= max_bytes,
                    "Notion account exceeds record bound"
                );
                let observation: AccountObservation = serde_json::from_str(&value)?;
                ensure!(
                    observation.id()? == intent.entity,
                    "Notion account identity mismatch"
                );
                record.provenance.provider = observation.account.provider().into();
                record.data = RecordData::Account {
                    observation: Da01Observation::from_account(&observation)?,
                };
            }
            "activity" => {
                let value: String = conn.query_row(
                    "SELECT manifest FROM activity_observations WHERE id=?1",
                    [&intent.entity],
                    |r| r.get(0),
                )?;
                ensure!(
                    value.len() <= max_bytes,
                    "Notion activity exceeds record bound"
                );
                let observation: ActivityObservation = serde_json::from_str(&value)?;
                ensure!(
                    observation.id == intent.entity,
                    "Notion activity identity mismatch"
                );
                record.original_urls = urls(
                    &conn,
                    &observation.activity.source.id(),
                    &observation.activity.source,
                )?;
                record.provenance.provider = observation.activity.actor.provider().into();
                record.provenance.completeness = observation.fetch_status;
                record.data = RecordData::Activity {
                    observation: Da01Observation::from_activity(&observation)?,
                };
            }
            "document" => {
                let value: String = conn.query_row(
                    "SELECT record FROM notion_documents WHERE id=?1",
                    [&intent.entity],
                    |r| r.get(0),
                )?;
                ensure!(
                    value.len() <= max_bytes,
                    "Notion document exceeds record bound"
                );
                record = serde_json::from_str(&value)?;
            }
            _ => anyhow::bail!("unknown Notion queue record kind"),
        }
        ensure!(
            serde_json::to_vec(&record)?.len() <= max_bytes,
            "Notion record exceeds bound"
        );
        Ok(record)
    }
}

/// Shared transaction seam for derived producers and semantic recovery.
pub(crate) fn save_document(tx: &Transaction<'_>, record: &Record) -> Result<String> {
    ensure!(
        matches!(
            record.data,
            RecordData::DailyManifest { .. }
                | RecordData::AgentOutput { .. }
                | RecordData::Checkpoint { .. }
        ),
        "expected a derived publication document"
    );
    let plan = record.plan(&crate::config::Notion::default())?;
    let encoded = serde_json::to_string(record)?;
    let old: Option<String> = tx
        .query_row(
            "SELECT record FROM notion_documents WHERE id=?1",
            [&plan.publication_key],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(old) = old {
        ensure!(
            old == encoded,
            "derived document identity already has different content"
        );
    } else {
        tx.execute(
            "INSERT INTO notion_documents(id,record) VALUES(?1,?2)",
            params![plan.publication_key, encoded],
        )?;
    }
    Ok(plan.publication_key)
}
