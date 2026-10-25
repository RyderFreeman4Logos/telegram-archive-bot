//! Local-only operator controls. Supplying a page never asserts that its body is correct.
use crate::{
    notion::notion_id,
    storage::{Store, now},
};
use anyhow::{Result, ensure};
use rusqlite::{TransactionBehavior, params};
use serde_json::{Value, json};

impl Store {
    pub fn notion_list(&self, limit: usize) -> Result<Vec<Value>> {
        ensure!(
            (1..=100).contains(&limit),
            "Notion list limit must be 1..100"
        );
        Ok(self.connection()?.prepare("SELECT id,kind,destination_key,publication_key,state,may_have_remote,attempts,create_attempts,page_id,candidate_page_id,error,available_at FROM notion_queue ORDER BY CASE state WHEN 'complete' THEN 1 ELSE 0 END,created_at,id LIMIT ?1")?.query_map([limit], |r|Ok(json!({
            "id":r.get::<_,String>(0)?,"kind":r.get::<_,String>(1)?,"destination_key":r.get::<_,Option<String>>(2)?,
            "publication_key":r.get::<_,Option<String>>(3)?,"state":r.get::<_,String>(4)?,"may_have_remote":r.get::<_,bool>(5)?,
            "attempts":r.get::<_,i64>(6)?,"create_attempts":r.get::<_,i64>(7)?,"page_id":r.get::<_,Option<String>>(8)?,
            "unverified_candidate_page_id":r.get::<_,Option<String>>(9)?,"error":r.get::<_,Option<String>>(10)?,"available_at":r.get::<_,i64>(11)?
        })))?.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Schedule read-only reconciliation. No request occurs in this command/transaction.
    pub fn reconcile_notion(&self, id: &str, page: Option<&str>) -> Result<()> {
        let _lock = self.notion_lock()?.ok_or_else(|| {
            anyhow::anyhow!("Notion worker is busy; try again after the current operation")
        })?;
        let candidate = page
            .map(|p| notion_id(p).ok_or_else(|| anyhow::anyhow!("invalid Notion page ID")))
            .transpose()?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (state, known): (String, Option<String>) = tx.query_row(
            "SELECT state,page_id FROM notion_queue WHERE id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(
            state != "creating",
            "unfinished attempt must be settled by the worker first"
        );
        ensure!(
            candidate
                .as_ref()
                .is_none_or(|p| known.as_ref().is_none_or(|k| k == p)),
            "cannot replace an observed page identity"
        );
        let candidate = candidate.filter(|_| known.is_none());
        tx.execute("UPDATE notion_queue SET state='reconcile',may_have_remote=1,candidate_page_id=?1,available_at=0,error='operator_reconcile' WHERE id=?2",params![candidate,id])?;
        tx.execute("UPDATE notion_pdf SET read_only=1 WHERE queue_id=?1", [id])?;
        tx.execute("INSERT INTO notion_operator_actions(queue_id,action,created_at,candidate_page_id) VALUES(?1,'reconcile',?2,?3)",params![id,now(),candidate])?;
        tx.commit()?;
        Ok(())
    }

    /// Re-arm only a known no-effect attempt, for example after correcting auth/tool bounds.
    pub fn retry_notion(&self, id: &str) -> Result<()> {
        let _lock = self.notion_lock()?.ok_or_else(|| {
            anyhow::anyhow!("Notion worker is busy; try again after the current operation")
        })?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut changed=tx.execute("UPDATE notion_queue SET state='ready',available_at=0,create_attempts=0,error='operator_retry' WHERE id=?1 AND may_have_remote=0 AND state NOT IN ('creating','complete')",[id])?;
        if changed == 0 {
            // PDF continuation is safe only when no attachment append is uncertain and a
            // status append/upload creation does not have an unknown remote identity.
            changed=tx.execute("UPDATE notion_queue SET state='ready',available_at=0,create_attempts=0,error='operator_retry' WHERE id=?1 AND kind='pdf_attachment' AND state!='creating' AND EXISTS(SELECT 1 FROM notion_pdf p WHERE p.queue_id=notion_queue.id AND p.attachment_dispatched=0 AND p.upload_state!='creating' AND (p.status_dispatched=0 OR p.status_block_id IS NOT NULL))",[id])?;
            if changed == 1 {
                tx.execute("UPDATE notion_pdf SET read_only=0 WHERE queue_id=?1", [id])?;
            }
        }
        ensure!(
            changed == 1,
            "only a known no-effect intent can be retried; uncertain writes require reconciliation"
        );
        tx.execute(
            "INSERT INTO notion_operator_actions(queue_id,action,created_at) VALUES(?1,'retry',?2)",
            params![id, now()],
        )?;
        tx.commit()?;
        Ok(())
    }
}
