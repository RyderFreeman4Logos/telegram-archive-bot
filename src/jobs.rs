use crate::{
    config::Config,
    digest,
    storage::{Store, now},
    telegram::Request,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, future::Future, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Fetch,
    Metadata,
    Translate,
    Download,
    Synthesize,
    Finalize,
    Archive,
}
impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::Metadata => "metadata",
            Self::Translate => "translate",
            Self::Download => "download",
            Self::Synthesize => "synthesize",
            Self::Finalize => "finalize",
            Self::Archive => "archive",
        }
    }
    fn parse(s: &str) -> Result<Self> {
        Ok(serde_json::from_value(Value::String(s.into()))?)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    Retryable,
    Complete,
    Partial,
    Failed,
    Cancelled,
    Paused,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub bot_id: i64,
    pub chat_id: i64,
    pub user_id: i64,
    pub request: Request,
    pub config_id: String,
    pub state: JobState,
    pub phase: Phase,
    pub attempt: u32,
    pub deadline: Option<i64>,
    pub error: Option<String>,
    pub result: Option<Value>,
    #[serde(default)]
    pub force: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct Scope {
    pub bot_id: i64,
    pub chat_id: i64,
    pub user_id: i64,
}

#[derive(Clone, Debug)]
pub struct Lease {
    pub job: Job,
    pub config: Config,
    pub tools: Value,
    generation: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Failure {
    Network,
    RateLimited,
    Unavailable,
    Timeout,
    InvalidInput,
    InvalidOutput,
    ToolMissing,
    ToolExit,
    ModelTruncated,
    ModelRefused,
    Cancelled,
    LeaseLost,
    DiskFull,
    Internal,
}
impl Failure {
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::Network | Self::RateLimited | Self::Unavailable | Self::Timeout
        )
    }
}
impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Failure {}

pub(crate) fn load_job(conn: &rusqlite::Connection, id: &str) -> Result<Job> {
    let mut statement = conn.prepare("SELECT j.id,j.bot_id,i.chat_id,i.user_id,j.request,j.config_id,j.state,j.phase,j.attempt,j.deadline,j.error,j.result,j.partial,j.force_run FROM jobs j JOIN inbox i ON i.bot_id=j.bot_id AND i.update_id=j.update_id WHERE j.id=?1")?;
    let mut rows = statement.query([id])?;
    let row = rows.next()?.context("job unavailable")?;
    let state: String = row.get(6)?;
    Ok(Job {
        id: row.get(0)?,
        bot_id: row.get(1)?,
        chat_id: row.get(2)?,
        user_id: row.get(3)?,
        request: serde_json::from_str(&row.get::<_, String>(4)?)?,
        config_id: row.get(5)?,
        state: if state == "complete" && row.get::<_, bool>(12)? {
            JobState::Partial
        } else {
            serde_json::from_value(Value::String(state))?
        },
        phase: Phase::parse(&row.get::<_, String>(7)?)?,
        attempt: row.get(8)?,
        deadline: row.get(9)?,
        error: row.get(10)?,
        force: row.get(13)?,
        result: row
            .get::<_, Option<String>>(11)?
            .map(|s| serde_json::from_str(&s))
            .transpose()?,
    })
}

pub(crate) fn fence(tx: &Transaction<'_>, lease: &Lease, time: i64) -> Result<()> {
    let (config, tools): (String, String) = tx.query_row(
        "SELECT config,tools FROM config_snapshots WHERE id=?1",
        [&lease.job.config_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let canonical: Config = serde_json::from_str(&config)?;
    ensure!(
        digest(format!("{config}\n{tools}")) == lease.job.config_id
            && serde_json::to_value(&canonical)? == serde_json::to_value(&lease.config)?
            && serde_json::from_str::<Value>(&tools)? == lease.tools,
        Failure::InvalidInput
    );
    let valid=tx.query_row("SELECT EXISTS(SELECT 1 FROM jobs WHERE id=?1 AND state='running' AND phase=?2 AND generation=?3 AND lease_until>?4 AND deadline>?4 AND cancel_requested=0 AND config_id=?5 AND request=?6 AND force_run=?7)",params![lease.job.id,lease.job.phase.as_str(),lease.generation,time,lease.job.config_id,serde_json::to_string(&lease.job.request)?,lease.job.force],|r|r.get::<_,bool>(0))?;
    ensure!(valid, Failure::LeaseLost);
    Ok(())
}

pub(crate) fn finish_in_tx(
    tx: &Transaction<'_>,
    lease: &Lease,
    output: &Value,
    next: Option<Phase>,
    partial: bool,
    time: i64,
) -> Result<()> {
    ensure!(
        next != Some(lease.job.phase),
        "phase cannot transition to itself"
    );
    fence(tx, lease, time)?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT output FROM phase_outputs WHERE job_id=?1 AND phase=?2",
            params![lease.job.id, lease.job.phase.as_str()],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(existing) = existing {
        ensure!(
            serde_json::from_str::<Value>(&existing)? == *output,
            "immutable phase output differs"
        );
    } else {
        tx.execute(
            "INSERT INTO phase_outputs VALUES(?1,?2,?3)",
            params![
                lease.job.id,
                lease.job.phase.as_str(),
                serde_json::to_string(output)?
            ],
        )?;
    }
    let state = if next.is_some() { "queued" } else { "complete" };
    tx.execute("UPDATE jobs SET state=?1,phase=?2,result=?3,partial=MAX(partial,?4),lease_until=NULL,available_at=0 WHERE id=?5",params![state,next.unwrap_or(lease.job.phase).as_str(),serde_json::to_string(output)?,partial,lease.job.id])?;
    Ok(())
}

pub(crate) fn scoped_job(conn: &rusqlite::Connection, scope: Scope, id: &str) -> Result<Job> {
    let job = load_job(conn, id)?;
    ensure!(
        (job.bot_id, job.chat_id, job.user_id) == (scope.bot_id, scope.chat_id, scope.user_id),
        "job unavailable in this scope"
    );
    Ok(job)
}
pub(crate) fn cancel_in_tx(tx: &Transaction<'_>, scope: Scope, id: &str) -> Result<bool> {
    scoped_job(tx, scope, id)?;
    let changed=tx.execute("UPDATE jobs SET state='cancelled',cancel_requested=1,generation=generation+1,lease_until=NULL WHERE id=?1 AND state IN ('queued','running','retryable','paused')",[id])?;
    let deliveries=tx.execute("UPDATE outbox SET state='terminal',error='cancelled_before_send' WHERE job_id=?1 AND state IN ('pending','retryable','paused')",[id])?;
    Ok(changed + deliveries > 0)
}
pub(crate) fn retry_in_tx(
    tx: &Transaction<'_>,
    scope: Scope,
    id: &str,
    phase: Phase,
    time: i64,
) -> Result<()> {
    let job = scoped_job(tx, scope, id)?;
    ensure!(
        matches!(
            job.state,
            JobState::Failed | JobState::Cancelled | JobState::Paused
        ) && job.phase == phase,
        "only the current stopped phase can be retried; refresh creates a new run"
    );
    ensure!(
        job.deadline.is_none_or(|d| d > time),
        "job deadline exhausted; submit a new run"
    );
    let max: u32 = tx
        .query_row(
            "SELECT attempts FROM phase_attempts WHERE job_id=?1 AND phase=?2",
            params![id, phase.as_str()],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);
    let config: String = tx.query_row(
        "SELECT config FROM config_snapshots WHERE id=?1",
        [job.config_id],
        |r| r.get(0),
    )?;
    ensure!(
        max < serde_json::from_str::<Config>(&config)?.limits.max_attempts,
        "phase attempt budget exhausted; submit a new run"
    );
    let changed=tx.execute("UPDATE jobs SET state='queued',cancel_requested=0,error=NULL,generation=generation+1,available_at=0 WHERE id=?1 AND state IN ('failed','cancelled','paused') AND phase=?2",params![id,phase.as_str()])?;
    ensure!(changed == 1, "job state changed concurrently");
    Ok(())
}

impl Store {
    pub fn claim_job(&self) -> Result<Option<Lease>> {
        self.claim_at(now())
    }

    pub fn claim_phase(&self, phase: Phase) -> Result<Option<Lease>> {
        self.claim_for_phase(now(), Some(phase))
    }
    fn claim_at(&self, time: i64) -> Result<Option<Lease>> {
        self.claim_for_phase(time, None)
    }
    pub(crate) fn claim_control(&self) -> Result<Option<Lease>> {
        self.claim_filtered(now(), Some(Phase::Fetch), Some(true))
    }
    pub(crate) fn claim_source(&self) -> Result<Option<Lease>> {
        self.claim_filtered(now(), Some(Phase::Fetch), Some(false))
    }
    fn claim_for_phase(&self, time: i64, phase: Option<Phase>) -> Result<Option<Lease>> {
        self.claim_filtered(time, phase, None)
    }
    fn claim_filtered(
        &self,
        time: i64,
        phase: Option<Phase>,
        control: Option<bool>,
    ) -> Result<Option<Lease>> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("UPDATE jobs SET state='failed',error='absolute_deadline_exhausted',generation=generation+1 WHERE state IN ('queued','retryable','running') AND deadline<=?1",[time])?;
        if tx.query_row("SELECT EXISTS(SELECT 1 FROM operational_holds)", [], |r| {
            r.get::<_, bool>(0)
        })? {
            tx.commit()?;
            return Ok(None);
        }
        let selected = loop {
            let id=tx.query_row("SELECT id FROM jobs WHERE cancel_requested=0 AND (?2 IS NULL OR phase=?2) AND (?3 IS NULL OR (json_extract(request,'$.kind')='command')=?3) AND ((state IN ('queued','retryable') AND available_at<=?1) OR (state='running' AND lease_until<=?1)) ORDER BY available_at,rowid LIMIT 1",params![time,phase.map(Phase::as_str),control],|r|r.get::<_,String>(0)).optional()?;
            let Some(id) = id else {
                tx.commit()?;
                return Ok(None);
            };
            let job = load_job(&tx, &id)?;
            let (config, tools): (String, String) = tx.query_row(
                "SELECT config,tools FROM config_snapshots WHERE id=?1",
                [&job.config_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let config: Config = serde_json::from_str(&config)?;
            let tools: Value = serde_json::from_str(&tools)?;
            let attempts: u32 = tx
                .query_row(
                    "SELECT attempts FROM phase_attempts WHERE job_id=?1 AND phase=?2",
                    params![id, job.phase.as_str()],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(0);
            if attempts >= config.limits.max_attempts {
                tx.execute("UPDATE jobs SET state='failed',error='phase_attempt_budget_exhausted',generation=generation+1 WHERE id=?1",[&id])?;
                continue;
            }
            break (job, config, tools);
        };
        let (job, config, tools) = selected;
        let deadline = job
            .deadline
            .unwrap_or(time.saturating_add(config.limits.job_seconds as i64));
        let lease_until = time
            .saturating_add(config.limits.lease_seconds as i64)
            .min(deadline);
        tx.execute("UPDATE jobs SET state='running',generation=generation+1,attempt=attempt+1,lease_until=?1,deadline=?2,error=NULL WHERE id=?3",params![lease_until,deadline,job.id])?;
        tx.execute("INSERT INTO phase_attempts VALUES(?1,?2,1) ON CONFLICT(job_id,phase) DO UPDATE SET attempts=attempts+1",params![job.id,job.phase.as_str()])?;
        let generation =
            tx.query_row("SELECT generation FROM jobs WHERE id=?1", [&job.id], |r| {
                r.get(0)
            })?;
        let job = load_job(&tx, &job.id)?;
        tx.commit()?;
        Ok(Some(Lease {
            job,
            config,
            tools,
            generation,
        }))
    }

    pub fn heartbeat(&self, lease: &Lease) -> Result<()> {
        self.heartbeat_at(lease, now())
    }
    fn heartbeat_at(&self, lease: &Lease, time: i64) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        fence(&tx, lease, time)?;
        tx.execute(
            "UPDATE jobs SET lease_until=MIN(?1,deadline) WHERE id=?2",
            params![
                time.saturating_add(lease.config.limits.lease_seconds as i64),
                lease.job.id
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn phase_outputs(&self, job_id: &str) -> Result<BTreeMap<String, Value>> {
        let conn = self.connection()?;
        let rows = conn
            .prepare("SELECT phase,output FROM phase_outputs WHERE job_id=?1 ORDER BY phase")?
            .query_map([job_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|(phase, json)| Ok((phase, serde_json::from_str(&json)?)))
            .collect()
    }

    pub fn finish_phase(
        &self,
        lease: &Lease,
        output: &Value,
        next: Option<Phase>,
        partial: bool,
    ) -> Result<()> {
        self.finish_at(lease, output, next, partial, now())
    }
    fn finish_at(
        &self,
        lease: &Lease,
        output: &Value,
        next: Option<Phase>,
        partial: bool,
        time: i64,
    ) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        finish_in_tx(&tx, lease, output, next, partial, time)?;
        tx.commit()?;
        Ok(())
    }

    pub fn fail_phase(&self, lease: &Lease, failure: Failure) -> Result<()> {
        self.fail_at(lease, failure, now())
    }
    fn fail_at(&self, lease: &Lease, failure: Failure, time: i64) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        fence(&tx, lease, time)?;
        let attempts: u32 = tx.query_row(
            "SELECT attempts FROM phase_attempts WHERE job_id=?1 AND phase=?2",
            params![lease.job.id, lease.job.phase.as_str()],
            |r| r.get(0),
        )?;
        let delay = (1i64 << attempts.min(8)) * 2;
        let state = if failure == Failure::Cancelled {
            "cancelled"
        } else if failure.retryable()
            && attempts < lease.config.limits.max_attempts
            && time.saturating_add(delay) < lease.job.deadline.unwrap_or(0)
        {
            "retryable"
        } else {
            "failed"
        };
        tx.execute(
            "UPDATE jobs SET state=?1,error=?2,available_at=?3,lease_until=NULL WHERE id=?4",
            params![
                state,
                serde_json::to_string(&failure)?,
                time.saturating_add(delay),
                lease.job.id
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn job_for(&self, scope: Scope, id: &str) -> Result<Job> {
        let job = load_job(&self.connection()?, id)?;
        ensure!(
            (job.bot_id, job.chat_id, job.user_id) == (scope.bot_id, scope.chat_id, scope.user_id),
            "job unavailable in this scope"
        );
        Ok(job)
    }

    pub fn cancel_job(&self, scope: Scope, id: &str) -> Result<bool> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = cancel_in_tx(&tx, scope, id)?;
        tx.commit()?;
        Ok(changed)
    }
    pub fn retry_job(&self, scope: Scope, id: &str, phase: Phase) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        retry_in_tx(&tx, scope, id, phase, now())?;
        tx.commit()?;
        Ok(())
    }

    pub fn workflow_mapping(&self, lease: &Lease, input_digest: &str) -> Result<String> {
        ensure!(input_digest.len() == 64, "invalid workflow input digest");
        let id = digest(format!(
            "{}:{}:{input_digest}",
            lease.job.id, lease.job.config_id
        ));
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        fence(&tx, lease, now())?;
        tx.execute("INSERT INTO workflow_runs(id,job_id,config_id,input_digest,graph_thread) VALUES(?1,?2,?3,?4,?1) ON CONFLICT DO NOTHING",params![id,lease.job.id,lease.job.config_id,input_digest])?;
        tx.commit()?;
        Ok(id)
    }
}

pub async fn supervise_lease<T, F, Fut>(
    store: Store,
    lease: Lease,
    shutdown: CancellationToken,
    work: F,
) -> std::result::Result<T, Failure>
where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = std::result::Result<T, Failure>>,
{
    let cancel = shutdown.child_token();
    let future = work(cancel.clone());
    tokio::pin!(future);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(
        (lease.config.limits.lease_seconds / 3).max(1),
    ));
    let failure = loop {
        tokio::select! {biased;
            _=shutdown.cancelled()=>break Failure::Cancelled,
            result=&mut future=>return result,
            _=heartbeat.tick()=>{
                if store.heartbeat(&lease).is_err() {break Failure::LeaseLost;}
            }
        }
    };
    cancel.cancel();
    let _ = tokio::time::timeout(
        Duration::from_secs(lease.config.limits.shutdown_seconds),
        &mut future,
    )
    .await;
    Err(failure)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{fs::Permissions, os::unix::fs::PermissionsExt};
    fn fixture() -> Result<(tempfile::TempDir, Store, Scope)> {
        let dir = tempfile::Builder::new()
            .permissions(Permissions::from_mode(0o700))
            .tempdir()?;
        let store = Store::open(dir.path())?;
        let mut config = Config::default();
        config.telegram.bot_id = 1;
        config.telegram.allowed_chats = vec![2];
        config.telegram.allowed_users = vec![3];
        let update = serde_json::from_value(
            json!({"update_id":1,"message":{"message_id":1,"chat":{"id":2},"from":{"id":3},"text":"https://example.org/"}}),
        )?;
        store.ingest(&config, &update, &json!({"ffmpeg":"synthetic-version"}))?;
        Ok((
            dir,
            store,
            Scope {
                bot_id: 1,
                chat_id: 2,
                user_id: 3,
            },
        ))
    }

    #[test]
    fn old_snapshot_defaults_preserve_hash_and_reject_claimed_mutation() -> Result<()> {
        let (_dir, store, _) = fixture()?;
        let conn = store.connection()?;
        let (config, tools): (String, String) = conn.query_row(
            "SELECT config,tools FROM config_snapshots LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut old: Value = serde_json::from_str(&config)?;
        old["translation"]
            .as_object_mut()
            .unwrap()
            .remove("backend");
        old["tools"].as_object_mut().unwrap().remove("bubblewrap");
        let old = serde_json::to_string(&old)?;
        let id = digest(format!("{old}\n{tools}"));
        conn.execute(
            "INSERT INTO config_snapshots VALUES(?1,?2,?3)",
            params![id, old, tools],
        )?;
        conn.execute("UPDATE jobs SET config_id=?1", [id])?;
        let mut lease = store.claim_at(1000)?.unwrap();
        assert_eq!(lease.config.translation.backend, "openai_compatible");
        store.heartbeat_at(&lease, 1001)?;
        lease.config.translation.backend = "llama_cpp".into();
        assert!(store.heartbeat_at(&lease, 1002).is_err());
        Ok(())
    }

    #[test]
    fn lease_expiry_fences_old_owner_and_preserves_deadline() -> Result<()> {
        let (_dir, store, _scope) = fixture()?;
        let first = store.claim_at(1000)?.unwrap();
        assert!(store.claim_at(1001)?.is_none());
        let second = store.claim_at(1061)?.unwrap();
        assert_eq!(first.job.deadline, second.job.deadline);
        assert!(
            store
                .finish_at(&first, &json!({"stale":true}), None, false, 1061)
                .is_err()
        );
        assert!(store.heartbeat_at(&first, 1061).is_err());
        store.finish_at(&second, &json!({"selected":true}), None, false, 1061)?;
        assert_eq!(
            store.phase_outputs(&second.job.id)?["fetch"],
            json!({"selected":true})
        );
        Ok(())
    }

    #[test]
    fn completed_expensive_phase_survives_reopen_and_partial_is_truthful() -> Result<()> {
        let (dir, store, scope) = fixture()?;
        let fetch = store.claim_at(1000)?.unwrap();
        let blob = store.publish_bytes(b"expensive retained artifact")?;
        store.finish_at(
            &fetch,
            &json!({"blob":blob.digest}),
            Some(Phase::Metadata),
            true,
            1001,
        )?;
        drop(store);
        let store = Store::open(dir.path())?;
        let metadata = store.claim_at(1002)?.unwrap();
        assert_eq!(metadata.job.phase, Phase::Metadata);
        assert_eq!(
            store.phase_outputs(&metadata.job.id)?["fetch"]["blob"],
            blob.digest
        );
        store.finish_at(&metadata, &json!({"done":true}), None, false, 1003)?;
        assert_eq!(
            store.job_for(scope, &metadata.job.id)?.state,
            JobState::Partial
        );
        assert!(
            store
                .job_for(
                    Scope {
                        chat_id: 999,
                        ..scope
                    },
                    &metadata.job.id
                )
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn retries_and_restarts_never_reset_budgets() -> Result<()> {
        let (dir, store, scope) = fixture()?;
        let first = store.claim_at(1000)?.unwrap();
        store.fail_at(&first, Failure::Network, 1001)?;
        assert!(store.claim_at(1002)?.is_none());
        let second = Store::open(dir.path())?.claim_at(1010)?.unwrap();
        assert_eq!(second.job.attempt, 2);
        store.fail_at(&second, Failure::Timeout, 1011)?;
        let third = store.claim_at(1020)?.unwrap();
        store.fail_at(&third, Failure::RateLimited, 1021)?;
        assert_eq!(store.job_for(scope, &third.job.id)?.state, JobState::Failed);
        assert!(store.claim_at(1030)?.is_none());
        assert!(store.retry_job(scope, &third.job.id, Phase::Fetch).is_err());
        Ok(())
    }

    #[test]
    fn simultaneous_claims_select_one_owner_and_deadline_expires_permanently() -> Result<()> {
        let (_dir, store, scope) = fixture()?;
        let handles = (0..8)
            .map(|_| {
                let s = store.clone();
                std::thread::spawn(move || s.claim_at(1000))
            })
            .collect::<Vec<_>>();
        let mut claimed = vec![];
        for handle in handles {
            if let Some(lease) = handle.join().unwrap()? {
                claimed.push(lease);
            }
        }
        assert_eq!(claimed.len(), 1);
        let lease = &claimed[0];
        assert!(store.claim_at(lease.job.deadline.unwrap())?.is_none());
        let job = store.job_for(scope, &lease.job.id)?;
        assert_eq!(job.state, JobState::Failed);
        assert_eq!(job.deadline, lease.job.deadline);
        assert_eq!(job.error.as_deref(), Some("absolute_deadline_exhausted"));
        Ok(())
    }

    #[tokio::test]
    async fn durable_cancel_propagates_and_shutdown_drains() -> Result<()> {
        let (_dir, store, scope) = fixture()?;
        let lease = store.claim_job()?.unwrap();
        let id = lease.job.id.clone();
        assert!(store.cancel_job(scope, &id)?);
        assert!(store.heartbeat(&lease).is_err());
        let observed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = observed.clone();
        let result = supervise_lease(
            store.clone(),
            lease,
            CancellationToken::new(),
            move |cancel| async move {
                cancel.cancelled().await;
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
        assert_eq!(result, Err(Failure::LeaseLost));
        assert!(observed.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(store.job_for(scope, &id)?.state, JobState::Cancelled);
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let (_dir, other, _) = fixture()?;
        let lease = other.claim_job()?.unwrap();
        assert_eq!(
            supervise_lease(other, lease, shutdown, |cancel| async move {
                cancel.cancelled().await;
                Ok(())
            })
            .await,
            Err(Failure::Cancelled)
        );
        Ok(())
    }
}
