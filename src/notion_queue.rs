//! DA06 durable outbox. Process work starts only after these transactions commit.
use crate::{
    config::Notion,
    notion::{Failure, PublicationError, Record, VerifiedPage},
    storage::{Store, now},
};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{fs::File, os::unix::fs::OpenOptionsExt};

#[derive(Clone, Debug)]
pub(crate) struct Intent {
    pub id: String,
    pub kind: String,
    pub entity: String,
    pub observation: Option<i64>,
    pub created_at: i64,
    pub record: Option<String>,
    pub may_have_remote: bool,
    pub attempts: i64,
    pub page_id: Option<String>,
    pub candidate_page_id: Option<String>,
}

fn enqueue(
    tx: &Transaction<'_>,
    kind: &str,
    entity: &str,
    observation: Option<i64>,
    time: i64,
    unknown: bool,
) -> Result<()> {
    tx.execute("INSERT OR IGNORE INTO notion_queue(id,kind,entity_id,observation_id,created_at,destination_key,destination,state,may_have_remote) SELECT ?1,?2,?3,?4,?5,destination_key,destination,?6,?7 FROM notion_binding",params![format!("{kind}:{entity}"),kind,entity,observation,time,if unknown {"reconcile"} else {"pending_configuration"},unknown])?;
    Ok(())
}

impl Store {
    /// Scan at most `limit` canonical records in one short transaction. Cursors survive restart.
    pub fn notion_bootstrap(&self, limit: usize) -> Result<usize> {
        ensure!(
            (1..=256).contains(&limit),
            "Notion bootstrap bound must be 1..256"
        );
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cursor: Option<(String,String)> = tx.query_row("SELECT name,cursor FROM notion_bootstrap WHERE done=0 ORDER BY CASE name WHEN 'observations' THEN 0 ELSE 1 END,name LIMIT 1", [], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
        let Some((name, cursor)) = cursor else {
            tx.commit()?;
            return Ok(0);
        };
        let unknown: bool = tx.query_row("SELECT bootstrap_reconcile OR EXISTS(SELECT 1 FROM operational_holds) FROM notion_binding", [], |r|r.get(0))?;
        let mut last = cursor.clone();
        let count = if name == "observations" {
            let rows = tx.prepare("SELECT id,revision_id,observed_at FROM observations WHERE id>CAST(?1 AS INTEGER) ORDER BY id LIMIT ?2")?.query_map(params![cursor,limit], |r|Ok((r.get::<_,i64>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,i64>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            for (id, revision, time) in &rows {
                enqueue(
                    &tx,
                    "observation",
                    &id.to_string(),
                    Some(*id),
                    *time,
                    unknown,
                )?;
                if let Some(revision) = revision {
                    enqueue(&tx, "revision", revision, Some(*id), *time, unknown)?;
                }
                last = id.to_string();
            }
            rows.len()
        } else {
            let (table, kind, manifest) = match name.as_str() {
                "account_observations" => ("account_observations", "account", "manifest"),
                "activity_observations" => ("activity_observations", "activity", "manifest"),
                "notion_documents" => ("notion_documents", "document", "record"),
                _ => anyhow::bail!("unknown Notion bootstrap namespace"),
            };
            let rows = tx
                .prepare(&format!(
                    "SELECT id,{manifest} FROM {table} WHERE id>?1 ORDER BY id LIMIT ?2"
                ))?
                .query_map(params![cursor, limit], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (id, manifest) in &rows {
                let v: Value = serde_json::from_str(manifest)?;
                enqueue(
                    &tx,
                    kind,
                    id,
                    v["source_observation_id"].as_i64(),
                    v["retrieved_at"].as_i64().unwrap_or(0),
                    unknown,
                )?;
                last = id.clone();
            }
            rows.len()
        };
        tx.execute(
            "UPDATE notion_bootstrap SET cursor=?1,done=?2 WHERE name=?3",
            params![last, count < limit, name],
        )?;
        tx.commit()?;
        Ok(count)
    }

    pub(crate) fn notion_lock(&self) -> Result<Option<File>> {
        use fs2::FileExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(self.root().join("notion-worker.lock"))?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(file)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn notion_configure(&self, config: &Notion) -> Result<Option<String>> {
        let destination = config
            .destination
            .as_ref()
            .filter(|d| d.identity().is_some());
        let key = destination.and_then(|d| d.identity());
        let encoded = destination.map(serde_json::to_string).transpose()?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("UPDATE notion_binding SET destination_key=?1,destination=?2 WHERE destination_key IS NOT ?1 OR destination IS NOT ?2",params![key,encoded])?;
        if key.is_some() {
            tx.execute("UPDATE notion_queue SET destination_key=?1,destination=?2 WHERE id IN (SELECT id FROM notion_queue WHERE destination_key IS NULL ORDER BY created_at,id LIMIT 64)",params![key,encoded])?;
        }
        // The OS lock proves no previous local worker still owns these unfinished attempts.
        tx.execute("UPDATE notion_attempts SET finished_at=?1,outcome='reconcile' WHERE finished_at IS NULL",[now()])?;
        tx.execute("UPDATE notion_queue SET state='reconcile',may_have_remote=1,error='interrupted_attempt' WHERE state='creating'",[])?;
        tx.commit()?;
        Ok(key)
    }

    pub(crate) fn notion_next(&self, destination: &str, config: &Notion) -> Result<Option<Intent>> {
        Ok(self.connection()?.query_row("SELECT id,kind,entity_id,observation_id,created_at,record,may_have_remote,attempts,page_id,candidate_page_id FROM notion_queue WHERE destination_key=?1 AND state NOT IN ('complete','conflict','creating') AND available_at<=?2 AND (may_have_remote=1 OR create_attempts<?3) ORDER BY available_at,created_at,id LIMIT 1",params![destination,now(),config.max_attempts], |r|Ok(Intent{id:r.get(0)?,kind:r.get(1)?,entity:r.get(2)?,observation:r.get(3)?,created_at:r.get(4)?,record:r.get(5)?,may_have_remote:r.get(6)?,attempts:r.get(7)?,page_id:r.get(8)?,candidate_page_id:r.get(9)?})).optional()?)
    }

    pub(crate) fn notion_begin(
        &self,
        intent: &mut Intent,
        record: &Record,
        config: &Notion,
    ) -> Result<()> {
        let plan = record.plan(config)?;
        let encoded = serde_json::to_string(record)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure!(
            !tx.query_row("SELECT EXISTS(SELECT 1 FROM operational_holds)", [], |r| {
                r.get::<_, bool>(0)
            })?,
            "Notion publication is held after recovery"
        );
        let changed=tx.execute("UPDATE notion_queue SET record=?1,publication_key=?2,expected_digest=?3,state='creating',may_have_remote=1,attempts=attempts+1,create_attempts=create_attempts+?4,error=NULL WHERE id=?5 AND attempts=?6 AND state NOT IN ('complete','creating')",params![encoded,plan.publication_key,plan.canonical_digest,!intent.may_have_remote,intent.id,intent.attempts])?;
        ensure!(changed == 1, "Notion attempt ownership changed");
        intent.attempts += 1;
        tx.execute(
            "INSERT INTO notion_attempts(queue_id,number,action,started_at) VALUES(?1,?2,?3,?4)",
            params![
                intent.id,
                intent.attempts,
                if intent.may_have_remote {
                    "reconcile"
                } else {
                    "create"
                },
                now()
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn notion_invalid_record(&self, intent: &Intent) -> Result<()> {
        let changed = self.connection()?.execute("UPDATE notion_queue SET state='conflict',error='record_invalid_or_over_limit' WHERE id=?1 AND attempts=?2 AND state NOT IN ('creating','complete')", params![intent.id,intent.attempts])?;
        ensure!(changed == 1, "Notion invalid record ownership changed");
        Ok(())
    }

    pub(crate) fn notion_settle(
        &self,
        intent: &Intent,
        config: &Notion,
        result: &Result<Option<VerifiedPage>, PublicationError>,
    ) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, may_have_remote, page, receipt, error, delay) = match result {
            Ok(Some(verified)) => {
                let (key,digest,dest):(String,String,String)=tx.query_row("SELECT publication_key,expected_digest,destination_key FROM notion_queue WHERE id=?1",[&intent.id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
                ensure!(
                    key == verified.publication_key
                        && digest == verified.canonical_digest
                        && dest == verified.destination_key,
                    "Notion receipt does not match pinned intent"
                );
                crate::notion_pdf::enqueue(&tx, intent, verified)?;
                (
                    "complete",
                    true,
                    Some(verified.page_id.clone()),
                    Some(serde_json::to_string(verified)?),
                    None,
                    0,
                )
            }
            Ok(None) => (
                "reconcile",
                true,
                intent.page_id.clone(),
                None,
                Some("marker_not_visible".to_owned()),
                config.retry_seconds,
            ),
            Err(failure) => {
                let uncertain = intent.may_have_remote
                    || failure.page_id.is_some()
                    || matches!(
                        failure.state,
                        Failure::Reconcile | Failure::Conflict | Failure::InvalidOutput
                    );
                let state = match failure.state {
                    Failure::BlockedAuth => "blocked_auth",
                    Failure::Conflict | Failure::InvalidPayload => "conflict",
                    Failure::PendingConfiguration
                    | Failure::UnsupportedTool
                    | Failure::Disabled
                    | Failure::CompiledOut => "pending_configuration",
                    _ if uncertain => "reconcile",
                    _ => "retry_wait",
                };
                (
                    state,
                    uncertain,
                    failure.page_id.clone().or_else(|| intent.page_id.clone()),
                    None,
                    Some(
                        serde_json::to_value(failure.state)?
                            .as_str()
                            .unwrap_or("unavailable")
                            .to_owned(),
                    ),
                    failure
                        .retry_after_seconds
                        .unwrap_or(config.retry_seconds)
                        .max(config.retry_seconds)
                        .min(86400),
                )
            }
        };
        let changed=tx.execute("UPDATE notion_queue SET state=?1,may_have_remote=?2,page_id=coalesce(?3,page_id),candidate_page_id=CASE WHEN ?1='complete' THEN NULL ELSE candidate_page_id END,receipt=?4,error=?5,available_at=?6 WHERE id=?7 AND attempts=?8 AND state='creating'",params![state,may_have_remote,page,receipt,error,now().saturating_add(delay as i64),intent.id,intent.attempts])?;
        ensure!(changed == 1, "Notion result ownership changed");
        if state == "pending_configuration" && !may_have_remote {
            // No create request could run; installing/fixing prerequisites must resume automatically.
            tx.execute(
                "UPDATE notion_queue SET create_attempts=max(create_attempts-1,0) WHERE id=?1",
                [&intent.id],
            )?;
        }
        tx.execute("UPDATE notion_attempts SET finished_at=?1,outcome=?2,page_id=?3,receipt=?4 WHERE queue_id=?5 AND number=?6 AND finished_at IS NULL",params![now(),state,page,receipt,intent.id,intent.attempts])?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn notion_progress(
        &self,
        intent: &Intent,
        progress: &crate::notion::PublicationProgress,
    ) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous: Option<String> = tx.query_row(
            "SELECT progress FROM notion_queue WHERE id=?1 AND attempts=?2 AND state='creating'",
            params![intent.id, intent.attempts],
            |r| r.get(0),
        )?;
        let mut progress = progress.clone();
        if let Some(previous) = previous {
            let previous: crate::notion::PublicationProgress = serde_json::from_str(&previous)?;
            ensure!(
                previous.page_id == progress.page_id,
                "Notion progress changed page identity"
            );
            progress.written_blocks = progress.written_blocks.max(previous.written_blocks);
        }
        let encoded = serde_json::to_string(&progress)?;
        let changed=tx.execute("UPDATE notion_queue SET page_id=?1,progress=?2 WHERE id=?3 AND attempts=?4 AND state='creating' AND (page_id IS NULL OR page_id=?1)",params![progress.page_id,encoded,intent.id,intent.attempts])?;
        ensure!(changed == 1, "Notion progress ownership changed");
        tx.execute("UPDATE notion_attempts SET page_id=?1,progress=?2 WHERE queue_id=?3 AND number=?4 AND finished_at IS NULL",params![progress.page_id,encoded,intent.id,intent.attempts])?;
        tx.commit()?;
        Ok(())
    }

    pub fn notion_metrics(&self) -> Result<Value> {
        let conn = self.connection()?;
        let counts = conn
            .prepare("SELECT state,count(*) FROM notion_queue GROUP BY state")?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))?
            .collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()?;
        let (pending,unbound,oldest):(u64,u64,Option<i64>)=conn.query_row("SELECT count(*),coalesce(sum(destination_key IS NULL),0),min(created_at) FROM notion_queue WHERE state!='complete'",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        let scans: u64 = conn.query_row(
            "SELECT count(*) FROM notion_bootstrap WHERE done=0",
            [],
            |r| r.get(0),
        )?;
        let pdf_upload = conn
            .prepare("SELECT upload_state,count(*) FROM notion_pdf GROUP BY upload_state")?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))?
            .collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()?;
        let pdf_attachment = conn
            .prepare("SELECT attachment_state,count(*) FROM notion_pdf GROUP BY attachment_state")?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))?
            .collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()?;
        Ok(
            json!({"pdf":{"upload":pdf_upload,"attachment":pdf_attachment},"states":counts,"pending":pending,"unbound":unbound,"oldest_pending_age_seconds":oldest.map(|t|now().saturating_sub(t).max(0)),"bootstrap_namespaces_remaining":scans}),
        )
    }
}
