//! DA07 / #74: retained originals and best-effort attachments share the Notion outbox.
use crate::{notion::VerifiedPage, notion_queue::Intent, sources::Identity, storage::now};
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const MAX_PDF_BYTES: u64 = 20 * 1024 * 1024;

/// Portable metadata: local CAS paths and signed URLs are never permanent identifiers.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PdfOriginal {
    pub source_id: String,
    pub revision_id: Option<String>,
    pub observation_id: i64,
    pub original_urls: Vec<String>,
    pub landing_urls: Vec<String>,
    pub landing_urls_omitted: usize,
    pub canonical_url: String,
    pub direct_url: Option<String>,
    pub direct_url_stability: String,
    pub mime: String,
    pub bytes: Option<u64>,
    pub sha256: String,
    pub filename: String,
    pub title: Option<String>,
    pub retrieved_at: i64,
    pub validated: bool,
    pub extraction_status: String,
    pub text_available: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PdfJob {
    pub schema_version: u32,
    pub original: PdfOriginal,
    pub text_page: VerifiedPage,
}

pub(crate) fn original(conn: &Connection, id: i64) -> Result<Option<PdfOriginal>> {
    let (source,revision,at,status,evidence,raw,provider,external):(String,Option<String>,i64,String,String,Option<String>,String,String)=conn.query_row("SELECT o.source_id,o.revision_id,o.observed_at,o.status,o.evidence,o.raw_digest,s.provider,s.external_id FROM observations o JOIN sources s ON s.id=o.source_id WHERE o.id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)))?;
    let evidence: Value = serde_json::from_str(&evidence)?;
    let retained = &evidence["original"];
    if retained["mime"] != "application/pdf" {
        return Ok(None);
    }
    let Some(raw) = raw.or_else(|| retained["digest"].as_str().map(str::to_owned)) else {
        return Ok(None);
    };
    let bytes = conn
        .query_row("SELECT size FROM blobs WHERE digest=?1", [&raw], |r| {
            r.get::<_, u64>(0)
        })
        .optional()?;
    let canonical_url = Identity {
        provider,
        external_id: external,
    }
    .url();
    let mut urls = conn
        .prepare("SELECT url FROM source_aliases WHERE source_id=?1 ORDER BY url")?
        .query_map([&source], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !urls.contains(&canonical_url) {
        urls.push(canonical_url.clone())
    }
    let landings=conn.prepare("SELECT DISTINCT s.provider,s.external_id FROM relations r JOIN sources s ON s.id=r.source_id WHERE r.target_id=?1 AND r.kind='links' ORDER BY s.id LIMIT 129")?.query_map([&source],|r|Ok(Identity{provider:r.get(0)?,external_id:r.get(1)?}.url()))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let count: usize = conn.query_row(
        "SELECT count(DISTINCT source_id) FROM relations WHERE target_id=?1 AND kind='links'",
        [&source],
        |r| r.get(0),
    )?;
    let omitted = count.saturating_sub(128);
    let landing_urls = landings.into_iter().take(128).collect();
    let metadata: Value = revision
        .as_ref()
        .map(|revision| {
            conn.query_row(
                "SELECT metadata FROM source_revisions WHERE id=?1",
                [revision],
                |r| r.get::<_, String>(0),
            )
        })
        .transpose()?
        .map(|s| serde_json::from_str(&s))
        .transpose()?
        .unwrap_or(Value::Null);
    let direct_url = evidence["resolved_url"]
        .as_str()
        .or_else(|| evidence["submitted_url"].as_str())
        .map(str::to_owned);
    let temporary = direct_url
        .as_deref()
        .and_then(|s| url::Url::parse(s).ok())
        .is_some_and(|u| {
            u.query_pairs().any(|(k, _)| {
                matches!(
                    k.to_ascii_lowercase().as_str(),
                    "signature" | "expires" | "token" | "x-amz-signature" | "x-amz-expires"
                )
            })
        });
    let text_available = revision.is_some() && matches!(status.as_str(), "complete" | "partial");
    Ok(Some(PdfOriginal {
        source_id: source,
        revision_id: revision,
        observation_id: id,
        original_urls: urls,
        landing_urls,
        landing_urls_omitted: omitted,
        canonical_url,
        direct_url,
        direct_url_stability: if temporary { "temporary" } else { "unknown" }.into(),
        mime: "application/pdf".into(),
        bytes,
        sha256: raw,
        filename: retained["display_name"]
            .as_str()
            .unwrap_or("original.pdf")
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or("original.pdf")
            .chars()
            .filter(|c| !c.is_control())
            .take(256)
            .collect(),
        title: metadata["title"].as_str().map(str::to_owned),
        retrieved_at: evidence["retrieved_at"].as_i64().unwrap_or(at),
        validated: retained["validated"].as_bool().unwrap_or(false),
        extraction_status: if text_available {
            status
        } else {
            "no_extractable_text".into()
        },
        text_available,
    }))
}

pub(crate) fn enqueue(tx: &Transaction<'_>, intent: &Intent, page: &VerifiedPage) -> Result<()> {
    if intent.kind != "observation" {
        return Ok(());
    }
    let Some(id) = intent.observation else {
        return Ok(());
    };
    let Some(original) = original(tx, id)? else {
        return Ok(());
    };
    let entity = crate::digest(format!(
        "{}:{}:{}",
        original.source_id, original.sha256, original.observation_id
    ));
    let key = format!("pdf_attachment:{entity}");
    let job = PdfJob {
        schema_version: 1,
        original,
        text_page: page.clone(),
    };
    let encoded = serde_json::to_string(&job)?;
    tx.execute("INSERT OR IGNORE INTO notion_queue(id,kind,entity_id,observation_id,created_at,destination_key,destination,publication_key,record,expected_digest,state,page_id) SELECT ?1,'pdf_attachment',?2,?3,?4,destination_key,destination,?1,?5,?6,'ready',?7 FROM notion_queue WHERE id=?8",params![key,entity,id,now(),encoded,job.original.sha256,page.page_id,intent.id])?;
    tx.execute(
        "INSERT OR IGNORE INTO notion_pdf(queue_id) VALUES(?1)",
        [key],
    )?;
    Ok(())
}

/// Day-manifest producers and text-only operators can consume this without local paths.
impl crate::storage::Store {
    /// Admit old verified observation pages once; new completions enqueue in their transaction.
    pub(crate) fn notion_pdf_bootstrap(&self, limit: usize) -> Result<usize> {
        ensure!(
            (1..=64).contains(&limit),
            "PDF bootstrap bound must be 1..64"
        );
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let cursor: Option<String> = tx
            .query_row(
                "SELECT cursor FROM notion_pdf_bootstrap WHERE done=0",
                [],
                |r| r.get(0),
            )
            .optional()?;
        let Some(cursor) = cursor else { return Ok(0) };
        let rows=tx.prepare("SELECT id,entity_id,observation_id,created_at,receipt FROM notion_queue WHERE kind='observation' AND state='complete' AND id>?1 ORDER BY id LIMIT ?2")?.query_map(params![cursor,limit],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<i64>>(2)?,r.get::<_,i64>(3)?,r.get::<_,String>(4)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut last = cursor;
        for (id, entity, observation, created_at, receipt) in &rows {
            let page: VerifiedPage = serde_json::from_str(receipt)?;
            let intent = Intent {
                id: id.clone(),
                kind: "observation".into(),
                entity: entity.clone(),
                observation: *observation,
                created_at: *created_at,
                record: None,
                may_have_remote: true,
                attempts: 0,
                page_id: Some(page.page_id.clone()),
                candidate_page_id: None,
            };
            enqueue(&tx, &intent, &page)?;
            last = id.clone();
        }
        tx.execute(
            "UPDATE notion_pdf_bootstrap SET cursor=?1,done=?2",
            params![last, rows.len() < limit],
        )?;
        tx.commit()?;
        Ok(rows.len())
    }
    pub fn notion_pdf_availability(&self, limit: usize) -> Result<Vec<Value>> {
        self.notion_pdf_availability_after(limit, None)
    }
    pub fn notion_pdf_availability_after(
        &self,
        limit: usize,
        after: Option<&str>,
    ) -> Result<Vec<Value>> {
        ensure!(
            after.is_none_or(|s| s.len() <= 512 && !s.chars().any(char::is_control)),
            "invalid PDF continuation"
        );
        ensure!(
            (1..=1000).contains(&limit),
            "PDF status bound must be 1..1000"
        );
        let conn = self.connection()?;
        let rows=conn.prepare("SELECT q.id,q.page_id,q.record FROM notion_pdf p JOIN notion_queue q ON q.id=p.queue_id WHERE q.id>?2 ORDER BY q.id LIMIT ?1")?.query_map(params![limit,after.unwrap_or("")],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter().map(|(id,page,raw)|{let job:PdfJob=serde_json::from_str(&raw)?;let state=read_state(&conn,&id)?;Ok(json!({"schema_version":1,"queue_id":id,"original":job.original,"page_id":page,"block_id":state.block_id,"upload_id":state.upload_id,"upload_expiry":state.upload_expiry,"status_block_id":state.status_block_id,"upload":state.upload_state,"attachment":state.attachment_state,"verified_sha256":state.verified_sha256,"verified_at":state.verified_at,"gap":state.reason,"read_only_reconciliation":state.read_only,"file_access_guaranteed":false}))}).collect()
    }
}

/// Each mutation is checkpointed before a subprocess; unknown append is read-only thereafter.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PdfState {
    pub read_only: bool,
    pub upload_state: String,
    pub attachment_state: String,
    pub upload_id: Option<String>,
    pub upload_expiry: Option<i64>,
    pub block_id: Option<String>,
    pub status_block_id: Option<String>,
    pub status_dispatched: bool,
    pub attachment_dispatched: bool,
    pub verified_sha256: Option<String>,
    pub verified_at: Option<i64>,
    pub reason: Option<String>,
}
fn read_state(conn: &Connection, id: &str) -> Result<PdfState> {
    Ok(conn.query_row("SELECT upload_state,attachment_state,upload_id,upload_expiry,block_id,status_block_id,status_dispatched,attachment_dispatched,verified_sha256,verified_at,reason,read_only FROM notion_pdf WHERE queue_id=?1",[id],|r|Ok(PdfState{read_only:r.get(11)?,upload_state:r.get(0)?,attachment_state:r.get(1)?,upload_id:r.get(2)?,upload_expiry:r.get(3)?,block_id:r.get(4)?,status_block_id:r.get(5)?,status_dispatched:r.get(6)?,attachment_dispatched:r.get(7)?,verified_sha256:r.get(8)?,verified_at:r.get(9)?,reason:r.get(10)?}))?)
}
impl crate::storage::Store {
    #[cfg(feature = "ntn")]
    pub(crate) fn pdf_state(&self, id: &str) -> Result<PdfState> {
        read_state(&*self.connection()?, id)
    }
    #[cfg(feature = "ntn")]
    fn pdf_begin(&self, intent: &mut Intent) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        ensure!(
            !tx.query_row("SELECT EXISTS(SELECT 1 FROM operational_holds)", [], |r| {
                r.get::<_, bool>(0)
            })?,
            "Notion work is recovery held"
        );
        let changed=tx.execute("UPDATE notion_queue SET state='creating',attempts=attempts+1,create_attempts=create_attempts+1,may_have_remote=1 WHERE id=?1 AND attempts=?2 AND state NOT IN ('creating','complete')",params![intent.id,intent.attempts])?;
        ensure!(changed == 1, "PDF ownership changed");
        intent.attempts += 1;
        tx.execute("INSERT INTO notion_attempts(queue_id,number,action,started_at) VALUES(?1,?2,'reconcile',?3)",params![intent.id,intent.attempts,now()])?;
        tx.commit()?;
        Ok(())
    }
    #[cfg(feature = "ntn")]
    fn pdf_progress(&self, intent: &Intent, state: &PdfState) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        ensure!(
            tx.query_row(
                "SELECT state='creating' AND attempts=?2 FROM notion_queue WHERE id=?1",
                params![intent.id, intent.attempts],
                |r| r.get::<_, bool>(0)
            )?,
            "PDF ownership changed"
        );
        tx.execute("UPDATE notion_pdf SET upload_state=?2,attachment_state=?3,upload_id=?4,upload_expiry=?5,block_id=?6,status_block_id=?7,status_dispatched=?8,attachment_dispatched=?9,verified_sha256=?10,verified_at=?11,reason=?12 WHERE queue_id=?1",params![intent.id,state.upload_state,state.attachment_state,state.upload_id,state.upload_expiry,state.block_id,state.status_block_id,state.status_dispatched,state.attachment_dispatched,state.verified_sha256,state.verified_at,state.reason])?;
        let encoded = serde_json::to_string(state)?;
        tx.execute(
            "UPDATE notion_queue SET progress=?1 WHERE id=?2",
            params![encoded, intent.id],
        )?;
        tx.execute("UPDATE notion_attempts SET progress=?1 WHERE queue_id=?2 AND number=?3 AND finished_at IS NULL",params![encoded,intent.id,intent.attempts])?;
        tx.commit()?;
        Ok(())
    }
}
#[cfg(feature = "ntn")]
pub(crate) async fn tick(
    store: &crate::storage::Store,
    config: &crate::config::Config,
    intent: &mut Intent,
    cancel: &tokio_util::sync::CancellationToken,
    http: &crate::network::PublicHttp,
) -> Result<()> {
    let job: PdfJob = serde_json::from_str(
        intent
            .record
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("PDF job missing"))?,
    )?;
    ensure!(
        job.schema_version == 1
            && job.text_page.page_id == intent.page_id.as_deref().unwrap_or_default(),
        "PDF job pin mismatch"
    );
    store.pdf_begin(intent)?;
    let mut state = store.pdf_state(&intent.id)?;
    let raw = if job.original.validated && job.original.bytes.is_some_and(|n| n <= MAX_PDF_BYTES) {
        store.read_blob(&job.original.sha256, MAX_PDF_BYTES).ok()
    } else {
        None
    };
    let create_attempts: i64 = store.connection()?.query_row(
        "SELECT create_attempts FROM notion_queue WHERE id=?1",
        [&intent.id],
        |r| r.get(0),
    )?;
    let adapter = crate::notion::Adapter::new(&config.notion, &store.root().join("work"));
    let persist = |state: &PdfState| {
        store
            .pdf_progress(intent, state)
            .map_err(|_| crate::notion::PublicationError::from(crate::notion::Failure::Reconcile))
    };
    let result = adapter
        .publish_pdf(
            &intent.id,
            &job,
            &mut state,
            raw.as_deref(),
            create_attempts <= config.notion.max_attempts as i64,
            cancel,
            http,
            &persist,
        )
        .await;
    if let Err(failure) = &result
        && state.reason.is_none()
    {
        state.reason = Some(
            serde_json::to_value(failure.state)?
                .as_str()
                .unwrap_or("unavailable")
                .into(),
        );
    }
    store.pdf_progress(intent, &state)?;
    let done =
        result.is_ok() && matches!(state.attachment_state.as_str(), "verified" | "unavailable");
    let exhausted = matches!(&result,Err(error) if error.state==crate::notion::Failure::Limit)
        && !state.attachment_dispatched;
    let outcome = if done {
        "complete"
    } else if exhausted {
        "conflict"
    } else {
        "reconcile"
    };
    let encoded = serde_json::to_string(&state)?;
    let mut conn = store.connection()?;
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let changed=tx.execute("UPDATE notion_queue SET state=?1,receipt=?2,error=?3,available_at=?4 WHERE id=?5 AND state='creating' AND attempts=?6",params![outcome,encoded,state.reason,now().saturating_add(config.notion.retry_seconds as i64),intent.id,intent.attempts])?;
    ensure!(changed == 1, "PDF settlement ownership changed");
    tx.execute("UPDATE notion_attempts SET finished_at=?1,outcome=?2,page_id=?3,receipt=?4 WHERE queue_id=?5 AND number=?6 AND finished_at IS NULL",params![now(),outcome,job.text_page.page_id,encoded,intent.id,intent.attempts])?;
    use crate::failure_reporting::{ErrorClass, Producer, Subsystem};
    use crate::notion::Failure;
    let class = match &result {
        Ok(())
            if state.attachment_state == "verified"
                && state.verified_sha256.as_deref() == Some(&job.original.sha256) =>
        {
            None
        }
        Ok(()) => Some(ErrorClass::Unavailable),
        Err(error) => Some(match error.state {
            Failure::Disabled
            | Failure::CompiledOut
            | Failure::PendingConfiguration
            | Failure::UnsupportedTool
            | Failure::BlockedAuth => ErrorClass::PendingConfiguration,
            Failure::RetryWait => ErrorClass::RateLimited,
            Failure::Conflict => ErrorClass::RemoteConflict,
            Failure::Cancelled => ErrorClass::Cancelled,
            Failure::InvalidPayload => ErrorClass::Internal,
            Failure::Limit => ErrorClass::Unavailable,
            Failure::InvalidOutput => ErrorClass::InvalidResponse,
            Failure::Reconcile | Failure::Unavailable => ErrorClass::Transport,
        }),
    };
    Producer {
        subsystem: Subsystem::Notion,
        provider: "notion",
        operation: "pdf_attachment",
        schema: "2026-03-11",
        stream: &format!("pdf:{}", intent.id),
        source: &intent.id,
        window: None,
    }
    .settle(&tx, &intent.attempts.to_string(), now(), class)?;
    tx.commit()?;
    Ok(())
}

#[cfg(all(test, feature = "ntn"))]
#[path = "notion_pdf_tests.rs"]
mod tests;
