//! DA04 / #71: autonomous originals; canonical bodies remain in source_revisions/CAS.
use crate::{
    activities::{ActivityCapture, ActivityIdentity, ActivityObservation},
    config::Config,
    digest,
    sources::{
        Capture, FetchStatus, Identity, Omission, Related, View, ViewEdge, ViewMember, ViewPolicy,
        save_view,
    },
    storage::{Store, now},
};
use anyhow::{Context, Result, ensure};
use futures_util::FutureExt;
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

fn snapshot(tx: &Transaction<'_>, config: &Config) -> Result<String> {
    let raw = serde_json::to_string(config)?;
    let tools = r#"{"capture":"DA04-v1"}"#;
    let id = digest(format!("{raw}\n{tools}"));
    tx.execute(
        "INSERT OR IGNORE INTO config_snapshots VALUES(?1,?2,?3)",
        params![id, raw, tools],
    )?;
    Ok(id)
}
impl Store {
    /// Bounded rowid reconciliation: the committed DA01 observation is the enqueue boundary.
    pub fn admit_activity_captures(&self, config: &Config, limit: usize) -> Result<usize> {
        config.validate()?;
        ensure!(
            limit > 0 && limit <= config.daily.capture.max_admissions,
            "invalid capture admission limit"
        );
        if !config.daily.capture.enabled || self.recovery_paused()? {
            return Ok(0);
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cursor: i64 = tx.query_row(
            "SELECT last_rowid FROM daily_capture_admission WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        let rows=tx.prepare("SELECT rowid,manifest FROM activity_observations WHERE rowid>?1 ORDER BY rowid LIMIT ?2")?.query_map(params![cursor,limit],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let config_id = snapshot(&tx, config)?;
        let mut admitted = 0;
        for (rowid, raw) in rows {
            let origin: ActivityObservation = serde_json::from_str(&raw)?;
            if origin.activity.source.provider == "x"
                && origin.provenance.get("da04_hydration").is_none()
            {
                let identity = &origin.activity.source;
                tx.execute("INSERT OR IGNORE INTO daily_capture_work(source_id,identity,config_id,state,next_at) VALUES(?1,?2,?3,'queued',?4)",params![identity.id(),serde_json::to_string(identity)?,config_id,now()])?;
                admitted += tx.execute(
                    "INSERT OR IGNORE INTO daily_capture_origins VALUES(?1,?2)",
                    params![origin.id, identity.id()],
                )?;
                tx.execute("UPDATE daily_capture_work SET render_pending=CASE WHEN state='complete' THEN 1 ELSE render_pending END WHERE source_id=?1",[identity.id()])?;
                // Known same-author reply discovery can extend an already captured thread without refetching its root.
                tx.execute("UPDATE daily_capture_work SET state='queued',cycle=cycle+1,config_id=?2,checkpoint='{}',attempts=0,next_at=?3,render_pending=0,render_cursor=0 WHERE state='complete' AND source_id IN (SELECT r.target_id FROM relations r JOIN activities child ON child.source_id=r.source_id JOIN activities parent ON parent.source_id=r.target_id AND parent.actor_id=child.actor_id JOIN accounts account ON account.id=parent.actor_id AND json_extract(account.manifest,'$.identity')='provider_id' WHERE r.source_id=?1 AND r.kind='replies_to' AND child.kind='reply' AND parent.kind!='repost' LIMIT 16)",params![identity.id(),config_id,now()])?;
            }
            tx.execute(
                "UPDATE daily_capture_admission SET last_rowid=?1 WHERE singleton=1",
                [rowid],
            )?;
        }
        tx.commit()?;
        Ok(admitted)
    }
    pub fn activity_capture_status(&self) -> Result<Value> {
        let c = self.connection()?;
        let states = c
            .prepare("SELECT state,count(*) FROM daily_capture_work GROUP BY state")?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))?
            .collect::<rusqlite::Result<std::collections::BTreeMap<_, _>>>()?;
        let origins: u64 = c.query_row("SELECT count(*) FROM daily_capture_origins", [], |r| {
            r.get(0)
        })?;
        let cursor: i64 = c.query_row(
            "SELECT last_rowid FROM daily_capture_admission WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        let outcomes=c.prepare("SELECT CASE WHEN state='complete' AND json_extract(result,'$.partial')=1 THEN 'partial' ELSE state END,count(*) FROM daily_capture_work GROUP BY 1")?.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,u64>(1)?)))?.collect::<rusqlite::Result<std::collections::BTreeMap<_,_>>>()?;
        Ok(
            json!({"sources":states,"outcomes":outcomes,"origins":origins,"admission_cursor":cursor,"external_effects_paused":self.recovery_paused()?}),
        )
    }
    pub fn retry_activity_capture(&self, config: &Config, source_id: &str) -> Result<Value> {
        config.validate()?;
        ensure!(config.daily.capture.enabled, "daily capture is disabled");
        ensure!(
            source_id.len() == 64 && source_id.bytes().all(|b| b.is_ascii_hexdigit()),
            "capture retry requires a source digest"
        );
        let mut c = self.connection()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let config_id = snapshot(&tx, config)?;
        let count=tx.execute("UPDATE daily_capture_work SET state='queued',attempts=0,next_at=0,cycle=cycle+1,checkpoint=CASE WHEN config_id=?2 THEN checkpoint ELSE '{}' END,config_id=?2,render_pending=0,reason='operator_retry' WHERE source_id=?1 AND state IN ('failed','blocked')",params![source_id,config_id])?;
        ensure!(
            count == 1,
            "capture retry requires a failed or blocked source"
        );
        tx.commit()?;
        Ok(json!({"source_id":source_id,"state":"queued","completed_nodes_retained":true}))
    }
}

/// Transport seam used by the autonomous worker and local, synthetic fixtures.
pub trait Collector: Send + Sync {
    fn collect<'a>(
        &'a self,
        store: &'a Store,
        config: &'a Config,
        source: &'a Identity,
        cancel: &'a CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<Capture>>>;
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct CaptureTick {
    pub admitted: usize,
    pub materialized: usize,
    pub captured: bool,
    pub paused: bool,
}

pub struct ConfiguredCollector;
impl Collector for ConfiguredCollector {
    fn collect<'a>(
        &'a self,
        store: &'a Store,
        config: &'a Config,
        source: &'a Identity,
        cancel: &'a CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<Capture>>> {
        async move {
            if source.provider == "x" {
                crate::x::XGraph::configured_daily(config)?
                    .capture_one(store, source, config, cancel)
                    .await
            } else {
                ensure!(source.provider == "web", "unsupported capture provider");
                let extraction = crate::documents::Documents {
                    store: store.clone(),
                    config: config.clone(),
                    http: Default::default(),
                }
                .url(&source.url(), cancel)
                .await?;
                Ok(vec![extraction.capture])
            }
        }
        .boxed()
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Node {
    identity: Identity,
    depth: usize,
    replies: usize,
    role: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Retained {
    identity: Identity,
    observation_id: i64,
    revision_id: Option<String>,
    authored_at: Option<String>,
    role: String,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
struct Progress {
    queue: VecDeque<Node>,
    retained: Vec<Retained>,
    edges: Vec<ViewEdge>,
    omissions: Vec<Omission>,
    bytes: u64,
    links: usize,
}
struct Lease {
    source: Identity,
    generation: i64,
    cycle: i64,
    config: Config,
    progress: Progress,
    attempts: u32,
    exhausted: bool,
}
fn fence(tx: &Transaction<'_>, lease: &Lease) -> Result<()> {
    ensure!(
        !tx.query_row("SELECT EXISTS(SELECT 1 FROM operational_holds)", [], |r| {
            r.get::<_, bool>(0)
        })?,
        "capture recovery hold"
    );
    let valid:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM daily_capture_lease WHERE singleton=1 AND source_id=?1 AND generation=?2 AND lease_until>?3)",params![lease.source.id(),lease.generation,now()],|r|r.get(0))?;
    ensure!(valid, "stale capture lease");
    Ok(())
}
fn claim(store: &Store, config: &Config) -> Result<Option<Lease>> {
    if store.recovery_paused()? {
        return Ok(None);
    }
    let mut c = store.connection()?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (owner, until): (Option<String>, i64) = tx.query_row(
        "SELECT source_id,lease_until FROM daily_capture_lease WHERE singleton=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if owner.is_some() && until > now() {
        return Ok(None);
    }
    if let Some(owner) = owner {
        tx.execute("UPDATE daily_capture_attempts SET state='interrupted',reason='lease_expired',finished_at=?2 WHERE source_id=?1 AND finished_at IS NULL",params![owner,now()])?;
        tx.execute("UPDATE daily_capture_work SET state='retry',reason='interrupted',next_at=0 WHERE source_id=?1 AND state='collecting'",[owner])?;
    }
    // Each state uses the ready index and returns at most one candidate.
    let mut ready: Option<(i64, String)> = None;
    for state in ["queued", "retry", "complete"] {
        let candidate=tx.query_row("SELECT next_at,source_id FROM daily_capture_work WHERE state=?1 AND next_at<=?2 AND render_pending=0 ORDER BY next_at,source_id LIMIT 1",params![state,now()],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?))).optional()?;
        if let Some(candidate) = candidate
            && ready.as_ref().is_none_or(|r| &candidate < r)
        {
            ready = Some(candidate);
        }
    }
    let Some((_, id)) = ready else {
        tx.commit()?;
        return Ok(None);
    };
    let state: String = tx.query_row(
        "SELECT state FROM daily_capture_work WHERE source_id=?1",
        [&id],
        |r| r.get(0),
    )?;
    if state == "complete" {
        let config_id = snapshot(&tx, config)?;
        tx.execute("UPDATE daily_capture_work SET state='queued',cycle=cycle+1,attempts=0,checkpoint='{}',config_id=?2 WHERE source_id=?1",params![id,config_id])?;
    }
    let (raw,config_raw,checkpoint,cycle,attempts):(String,String,String,i64,u32)=tx.query_row("SELECT w.identity,c.config,w.checkpoint,w.cycle,w.attempts FROM daily_capture_work w JOIN config_snapshots c ON c.id=w.config_id WHERE source_id=?1",[&id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
    let config: Config = serde_json::from_str(&config_raw)?;
    config.validate()?;
    let source: Identity = serde_json::from_str(&raw)?;
    let mut progress: Progress = serde_json::from_str(&checkpoint)?;
    if progress.queue.is_empty() && progress.retained.is_empty() {
        progress.queue.push_back(Node {
            identity: source.clone(),
            depth: 0,
            replies: 0,
            role: "root".into(),
        });
    }
    tx.execute("UPDATE daily_capture_lease SET source_id=?1,generation=generation+1,lease_until=?2 WHERE singleton=1",params![id,now().saturating_add(config.limits.download_seconds.min(i64::MAX as u64-10) as i64+10)])?;
    let generation = tx.query_row(
        "SELECT generation FROM daily_capture_lease WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let exhausted = attempts >= config.limits.max_attempts;
    if !exhausted {
        tx.execute("UPDATE daily_capture_work SET state='collecting',attempts=attempts+1,reason=NULL WHERE source_id=?1",[&id])?;
        tx.execute("INSERT INTO daily_capture_attempts(source_id,cycle,attempt,generation,config_id,started_at,state) SELECT source_id,cycle,attempts,?2,config_id,?3,'collecting' FROM daily_capture_work WHERE source_id=?1",params![id,generation,now()])?;
    }
    tx.commit()?;
    Ok(Some(Lease {
        source,
        generation,
        cycle,
        config,
        progress,
        attempts: if exhausted { attempts } else { attempts + 1 },
        exhausted,
    }))
}
fn checkpoint(store: &Store, lease: &Lease) -> Result<()> {
    let raw = serde_json::to_string(&lease.progress)?;
    ensure!(
        raw.len() as u64 <= lease.config.limits.work_bytes.min(64 * 1024 * 1024),
        "capture checkpoint byte limit"
    );
    let mut c = store.connection()?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    fence(&tx, lease)?;
    tx.execute(
        "UPDATE daily_capture_work SET checkpoint=?2 WHERE source_id=?1",
        params![lease.source.id(), raw],
    )?;
    tx.execute("UPDATE daily_capture_attempts SET checkpoint=?4 WHERE source_id=?1 AND cycle=?2 AND attempt=?3 AND finished_at IS NULL",params![lease.source.id(),lease.cycle,lease.attempts,raw])?;
    tx.commit()?;
    Ok(())
}
fn cached(store: &Store, source: &Identity, refresh: u64) -> Result<Option<Vec<(i64, Capture)>>> {
    let c = store.connection()?;
    let row: Option<(i64, String)> = c
        .query_row(
            "SELECT observed_at,observation_ids FROM daily_capture_cache WHERE source_id=?1",
            [source.id()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((at, ids)) = row else {
        return Ok(None);
    };
    if now().saturating_sub(at) >= refresh as i64 {
        return Ok(None);
    }
    let ids: Vec<i64> = serde_json::from_str(&ids)?;
    let mut result = vec![];
    for id in ids {
        result.push((id, observation_capture(store, id)?));
    }
    Ok(Some(result))
}
fn observation_capture(store: &Store, id: i64) -> Result<Capture> {
    let c = store.connection()?;
    type Row = (
        String,
        Option<String>,
        String,
        String,
        String,
        Option<String>,
    );
    let (source,revision,status,extractor,evidence,raw):Row=c.query_row("SELECT source_id,revision_id,status,extractor,evidence,raw_digest FROM observations WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
    let identity: Identity = c.query_row(
        "SELECT provider,external_id FROM sources WHERE id=?1",
        [source],
        |r| {
            Ok(Identity {
                provider: r.get(0)?,
                external_id: r.get(1)?,
            })
        },
    )?;
    let content = revision
        .map(|id| store.revision(&id).map(|r| r.content))
        .transpose()?;
    let relations = c
        .prepare(
            "SELECT target_id,kind FROM relations WHERE observation_id=?1 ORDER BY kind,target_id",
        )?
        .query_map([id], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|(id, kind)| {
            let target = c.query_row(
                "SELECT provider,external_id FROM sources WHERE id=?1",
                [id],
                |r| {
                    Ok(Identity {
                        provider: r.get(0)?,
                        external_id: r.get(1)?,
                    })
                },
            )?;
            Ok(Related {
                target,
                kind: serde_json::from_value(json!(kind))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let evidence: Value = serde_json::from_str(&evidence)?;
    Ok(Capture {
        identity,
        submitted_url: None,
        content,
        status: serde_json::from_value(json!(status))?,
        extractor,
        evidence,
        raw_digest: raw,
        relations,
    })
}
fn retain_node(
    store: &Store,
    lease: &Lease,
    source: &Identity,
    captures: &[Capture],
) -> Result<Vec<(i64, Capture)>> {
    ensure!(
        captures.iter().any(|c| &c.identity == source),
        "collector did not return requested source"
    );
    let mut c = store.connection()?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    fence(&tx, lease)?;
    let mut ids = vec![];
    let mut result = vec![];
    for capture in captures {
        // An embedded preview must never replace already retained full text.
        if capture.extractor=="embedded-quote-preview"&&tx.query_row("SELECT EXISTS(SELECT 1 FROM sources s JOIN observations o ON o.source_id=s.id AND o.revision_id=s.current_revision WHERE s.id=?1 AND o.status='complete')",[capture.identity.id()],|r|r.get::<_,bool>(0))? {continue;}
        store.capture_in_tx(&tx, capture)?;
        let id: i64 = tx.query_row(
            "SELECT max(id) FROM observations WHERE source_id=?1",
            [capture.identity.id()],
            |r| r.get(0),
        )?;
        if &capture.identity == source {
            ids.push(id);
            result.push((id, capture.clone()));
        }
    }
    if result.iter().any(|(_, c)| c.status != FetchStatus::Error) {
        tx.execute("INSERT INTO daily_capture_cache VALUES(?1,?2,?3) ON CONFLICT(source_id) DO UPDATE SET observed_at=excluded.observed_at,observation_ids=excluded.observation_ids",params![source.id(),now(),serde_json::to_string(&ids)?])?;
    }
    tx.commit()?;
    Ok(result)
}
fn continuations(store: &Store, source: &Identity, limit: usize) -> Result<Vec<Identity>> {
    let c = store.connection()?;
    let ids=c.prepare("SELECT DISTINCT child.source_id FROM activities parent JOIN accounts account ON account.id=parent.actor_id AND json_extract(account.manifest,'$.identity')='provider_id' JOIN activities child ON child.actor_id=parent.actor_id AND child.kind='reply' JOIN relations r ON r.source_id=child.source_id AND r.target_id=parent.source_id AND r.kind='replies_to' WHERE parent.source_id=?1 AND parent.kind!='repost' ORDER BY child.source_id LIMIT ?2")?.query_map(params![source.id(),limit],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    ids.into_iter()
        .map(|id| {
            Ok(c.query_row(
                "SELECT provider,external_id FROM sources WHERE id=?1",
                [id],
                |r| {
                    Ok(Identity {
                        provider: r.get(0)?,
                        external_id: r.get(1)?,
                    })
                },
            )?)
        })
        .collect()
}
fn build_view(lease: &Lease) -> Result<View> {
    let mut members = lease
        .progress
        .retained
        .iter()
        .map(|r| ViewMember {
            source_id: r.identity.id(),
            revision_id: r.revision_id.clone(),
            missing_reason: if r.revision_id.is_none() {
                Some("source_unavailable".into())
            } else {
                None
            },
        })
        .collect::<Vec<_>>();
    if !members.iter().any(|m| m.source_id == lease.source.id()) {
        members.insert(
            0,
            ViewMember {
                source_id: lease.source.id(),
                revision_id: None,
                missing_reason: Some("capture_unfinished".into()),
            },
        );
    }
    Ok(View {
        schema_version: 1,
        root_id: lease.source.id(),
        policy: ViewPolicy {
            max_nodes: lease.config.limits.graph_nodes,
            max_depth: lease.config.limits.graph_depth,
        },
        members,
        edges: lease.progress.edges.clone(),
        omissions: lease.progress.omissions.clone(),
        retrieval: Some(
            json!({"adapter":"daily-capture-v1","contexts":lease.progress.retained,"scope":"known_references_and_known_same_author_replies_only","all_user_replies_enumerated":false,"bytes":lease.progress.bytes,"max_links":lease.config.daily.capture.max_links,"reply_ancestors":lease.config.limits.reply_ancestors}),
        ),
    })
}
fn finish(store: &Store, lease: &Lease, error: Option<&str>) -> Result<()> {
    // Freeze the view from this attempt's pinned observations before writing its terminal receipt.
    let prepared_view = if error.is_none() || lease.attempts >= lease.config.limits.max_attempts {
        Some(build_view(lease)?)
    } else {
        None
    };
    let mut c = store.connection()?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    fence(&tx, lease)?;
    tx.execute("UPDATE daily_capture_attempts SET state=?4,reason=?5,finished_at=?6,checkpoint=?7 WHERE source_id=?1 AND cycle=?2 AND attempt=?3 AND finished_at IS NULL",params![lease.source.id(),lease.cycle,lease.attempts,if error.is_some(){"failed"}else{"complete"},error,now(),serde_json::to_string(&lease.progress)?])?;
    if let Some(reason) = error {
        if lease.attempts < lease.config.limits.max_attempts {
            tx.execute("UPDATE daily_capture_work SET state='retry',reason=?2,next_at=?3 WHERE source_id=?1",params![lease.source.id(),reason,now().saturating_add(lease.config.daily.capture.retry_seconds as i64)])?;
        } else {
            let mut view = prepared_view.context("capture failure view")?;
            for pending in &lease.progress.queue {
                view.omissions.push(Omission {
                    source_id: pending.identity.id(),
                    reason: reason.into(),
                });
            }
            view.omissions.push(Omission {
                source_id: lease.source.id(),
                reason: "capture_attempt_budget_exhausted".into(),
            });
            save_view(&tx, &view)?;
            tx.execute("UPDATE daily_capture_work SET state='failed',reason=?2,result=?3,render_pending=1,render_cursor=0 WHERE source_id=?1",params![lease.source.id(),reason,serde_json::to_string(&json!({"view_id":view.id()?,"cycle":lease.cycle,"partial":true,"finished_at":now(),"failure":reason}))?])?;
        }
    } else {
        let view = prepared_view.context("capture completed view")?;
        save_view(&tx, &view)?;
        tx.execute("UPDATE daily_capture_work SET state='complete',reason=NULL,result=?2,next_at=?3,render_pending=1,render_cursor=0 WHERE source_id=?1",params![lease.source.id(),serde_json::to_string(&json!({"view_id":view.id()?,"cycle":lease.cycle,"partial":!view.omissions.is_empty(),"finished_at":now()}))?,now().saturating_add(lease.config.daily.capture.refresh_seconds as i64)])?;
    }
    tx.execute(
        "UPDATE daily_capture_lease SET source_id=NULL,lease_until=0 WHERE singleton=1",
        [],
    )?;
    tx.commit()?;
    Ok(())
}
fn capture_bytes(store: &Store, captures: &[Capture]) -> Result<u64> {
    let mut used = 0u64;
    let mut raw = std::collections::BTreeSet::new();
    for capture in captures {
        used = used.saturating_add(capture.content.as_ref().map_or(0, |c| c.body.len() as u64));
        if let Some(digest) = &capture.raw_digest
            && raw.insert(digest)
        {
            used = used.saturating_add(store.verify_blob(digest)?.size);
        }
    }
    Ok(used)
}
async fn traverse(
    store: &Store,
    lease: &mut Lease,
    collector: &dyn Collector,
    cancel: &CancellationToken,
) -> Result<()> {
    while let Some(node) = lease.progress.queue.front().cloned() {
        if cancel.is_cancelled() {
            anyhow::bail!("capture_cancelled")
        }
        if lease
            .progress
            .retained
            .iter()
            .any(|r| r.identity == node.identity)
        {
            lease.progress.queue.pop_front();
            continue;
        }
        let reason = if lease.progress.retained.len() >= lease.config.limits.graph_nodes {
            Some("node_limit")
        } else if node.depth > lease.config.limits.graph_depth {
            Some("depth_limit")
        } else if node.replies > lease.config.limits.reply_ancestors {
            Some("reply_ancestor_limit")
        } else if node.identity.provider == "web"
            && lease.progress.links >= lease.config.daily.capture.max_links
        {
            Some("external_link_limit")
        } else if lease.progress.bytes >= lease.config.limits.input_bytes {
            Some("byte_limit")
        } else {
            None
        };
        if let Some(reason) = reason {
            lease.progress.omissions.push(Omission {
                source_id: node.identity.id(),
                reason: reason.into(),
            });
            lease.progress.queue.pop_front();
            checkpoint(store, lease)?;
            continue;
        }
        let captures = if let Some(cached) = cached(
            store,
            &node.identity,
            lease.config.daily.capture.refresh_seconds,
        )? {
            cached
        } else {
            let mut bounded = lease.config.clone();
            bounded.limits.input_bytes = bounded
                .limits
                .input_bytes
                .saturating_sub(lease.progress.bytes);
            let mut captures = collector
                .collect(store, &bounded, &node.identity, cancel)
                .await?;
            let bytes = capture_bytes(store, &captures)?;
            if bytes > bounded.limits.input_bytes {
                // Keep provider raw evidence, never label oversized normalized text Complete.
                captures.retain(|c| c.identity == node.identity);
                for capture in &mut captures {
                    capture.content = None;
                    capture.status = FetchStatus::Missing;
                    capture.evidence["limitations"] = json!(["byte_limit"]);
                    capture.evidence["omitted_capture_bytes"] = json!(bytes);
                    capture.evidence["raw_retained"] = json!(capture.raw_digest.is_some());
                }
            }
            retain_node(store, lease, &node.identity, &captures)?
        };
        ensure!(
            captures.iter().any(|(_, c)| c.status != FetchStatus::Error),
            "capture provider failure"
        );
        let revision_id = store.current_revision(&node.identity)?;
        let revision = revision_id
            .as_ref()
            .map(|id| store.revision(id))
            .transpose()?;
        let captured_body_bytes: u64 = captures
            .iter()
            .filter_map(|(_, c)| c.content.as_ref())
            .map(|c| c.body.len() as u64)
            .sum();
        let selected_body_bytes = revision.as_ref().map_or(0, |r| r.content.body.len() as u64);
        let used = capture_bytes(
            store,
            &captures.iter().map(|(_, c)| c.clone()).collect::<Vec<_>>(),
        )?
        .saturating_add(selected_body_bytes.saturating_sub(captured_body_bytes));
        if used
            > lease
                .config
                .limits
                .input_bytes
                .saturating_sub(lease.progress.bytes)
        {
            lease.progress.omissions.push(Omission {
                source_id: node.identity.id(),
                reason: "byte_limit".into(),
            });
            lease.progress.queue.pop_front();
            checkpoint(store, lease)?;
            continue;
        }
        lease.progress.bytes = lease.progress.bytes.saturating_add(used);
        let mut relations = vec![];
        for (observation, capture) in &captures {
            if capture.status != FetchStatus::Complete {
                lease.progress.omissions.push(Omission {
                    source_id: node.identity.id(),
                    reason: format!(
                        "source_{}",
                        serde_json::to_value(capture.status)?.as_str().unwrap()
                    ),
                });
            }
            if capture.evidence["media_text_status"] == "references_only_not_transcribed" {
                lease.progress.omissions.push(Omission {
                    source_id: node.identity.id(),
                    reason: "media_references_not_transcribed".into(),
                });
            }
            if let Some(items) = capture.evidence["limitations"].as_array() {
                for item in items {
                    if let Some(reason) = item.as_str() {
                        lease.progress.omissions.push(Omission {
                            source_id: node.identity.id(),
                            reason: reason.into(),
                        });
                    }
                }
            }
            for relation in &capture.relations {
                lease.progress.edges.push(ViewEdge {
                    source_id: node.identity.id(),
                    target_id: relation.target.id(),
                    kind: serde_json::to_value(relation.kind)?
                        .as_str()
                        .context("relation kind")?
                        .into(),
                    observation_id: *observation,
                });
                relations.push(relation.clone());
            }
        }
        if node.identity.provider == "web" {
            lease.progress.links += 1;
        }
        let authored_at = revision.and_then(|r| r.content.metadata.authored_at);
        lease.progress.retained.push(Retained {
            identity: node.identity.clone(),
            observation_id: captures.last().context("missing source observation")?.0,
            revision_id,
            authored_at,
            role: node.role.clone(),
        });
        lease.progress.queue.pop_front();
        for relation in relations {
            let replies = if relation.kind == crate::sources::RelationKind::RepliesTo {
                node.replies + 1
            } else {
                0
            };
            lease.progress.queue.push_back(Node {
                identity: relation.target,
                depth: node.depth + 1,
                replies,
                role: "referenced_context".into(),
            });
        }
        for child in continuations(store, &node.identity, lease.config.limits.graph_nodes + 1)? {
            lease.progress.queue.push_back(Node {
                identity: child,
                depth: node.depth + 1,
                replies: 0,
                role: "known_same_author_continuation".into(),
            });
        }
        checkpoint(store, lease)?;
    }
    Ok(())
}

fn materialize(store: &Store, limit: usize) -> Result<usize> {
    if store.recovery_paused()? {
        return Ok(0);
    }
    let c = store.connection()?;
    let job:Option<(String,String,i64,i64)>=c.query_row("SELECT source_id,result,cycle,render_cursor FROM daily_capture_work WHERE render_pending=1 ORDER BY source_id LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
    let Some((source, raw, cycle, cursor)) = job else {
        return Ok(0);
    };
    let result: Value = serde_json::from_str(&raw)?;
    let history=c.prepare("SELECT attempt,state,reason,started_at,finished_at FROM daily_capture_attempts WHERE source_id=?1 AND cycle=?2 ORDER BY attempt LIMIT 100")?.query_map(params![source,cycle],|r|Ok(json!({"attempt":r.get::<_,i64>(0)?,"state":r.get::<_,String>(1)?,"reason":r.get::<_,Option<String>>(2)?,"started_at":r.get::<_,i64>(3)?,"finished_at":r.get::<_,Option<i64>>(4)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let base = store.view(result["view_id"].as_str().context("missing capture view")?)?;
    let origins=c.prepare("SELECT o.rowid,a.manifest FROM daily_capture_origins o JOIN activity_observations a ON a.id=o.origin_id WHERE o.source_id=?1 AND o.rowid>?2 ORDER BY o.rowid LIMIT ?3")?.query_map(params![source,cursor,limit],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut last = cursor;
    for (rowid, raw) in &origins {
        let origin: ActivityObservation = serde_json::from_str(raw)?;
        let mut view = base.clone();
        let mut retrieval = view.retrieval.clone().unwrap_or(json!({}));
        let bounds = &origin.discovery.provenance;
        let start = bounds["utc_start"].as_i64();
        let end = bounds["utc_end"].as_i64();
        if let Some(contexts) = retrieval["contexts"].as_array_mut() {
            for context in contexts {
                let at = context["authored_at"]
                    .as_str()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|d| d.timestamp());
                context["time_relation"] = json!(match (at, start, end) {
                    (Some(t), Some(a), Some(b)) if t >= a && t < b => "inside_window",
                    (Some(_), Some(_), Some(_)) => "outside_window",
                    _ => "unknown",
                });
            }
        }
        retrieval["attempt_history"] = json!(history);
        retrieval["selected_window"] = bounds.clone();
        retrieval["origin_observation_id"] = json!(origin.id);
        view.retrieval = Some(retrieval);
        let source_observation_id = base
            .retrieval
            .as_ref()
            .and_then(|r| r["contexts"].as_array())
            .and_then(|rows| {
                rows.iter().find(|row| {
                    row["identity"]
                        == serde_json::to_value(&origin.activity.source).unwrap_or(Value::Null)
                })
            })
            .and_then(|row| row["observation_id"].as_i64())
            .unwrap_or(origin.source_observation_id);
        let capture = observation_capture(store, source_observation_id)?;
        let (event, occurrence) = match &origin.activity.identity {
            ActivityIdentity::ProviderId { event_id, .. } => {
                (Some(event_id.clone()), origin.id.clone())
            }
            ActivityIdentity::Unresolved { occurrence_key, .. } => (None, occurrence_key.clone()),
        };
        let input = ActivityCapture {
            actor: origin.activity.actor.clone(),
            handle: origin.account_observation.handle.clone(),
            display_name: origin.account_observation.display_name.clone(),
            provider_event_id: event,
            kind: origin.activity.kind,
            discovery: origin.discovery.clone(),
            occurrence_key: occurrence,
            observation_key: format!("da04:{cycle}:{}:{}", origin.id, view.id()?),
            activity_time: origin.activity_time.clone(),
            source_time: origin.source_time.clone(),
            retrieved_at: result["finished_at"]
                .as_i64()
                .context("capture completion time")?,
            provenance: json!({"original":origin.provenance,"da04_hydration":{"origin_observation_id":origin.id,"result":result}}),
            capture,
            view_policy: view.policy.clone(),
        };
        store.record_activity_with_view(&input, &view, source_observation_id)?;
        last = *rowid;
    }
    c.execute("UPDATE daily_capture_work SET render_cursor=?3,render_pending=?4 WHERE source_id=?1 AND cycle=?2",params![source,cycle,last,origins.len()==limit])?;
    Ok(origins.len())
}

/// Actual bounded worker step: admission, pending origin receipts, then one global source lease.
pub async fn step_with(
    store: &Store,
    config: &Config,
    collector: &dyn Collector,
    cancel: &CancellationToken,
) -> Result<CaptureTick> {
    config.validate()?;
    let mut report = CaptureTick::default();
    if !config.daily.capture.enabled {
        return Ok(report);
    }
    if store.recovery_paused()? {
        report.paused = true;
        return Ok(report);
    }
    report.admitted = store.admit_activity_captures(config, config.daily.capture.max_admissions)?;
    report.materialized = materialize(store, config.daily.capture.max_admissions)?;
    if report.materialized > 0 {
        return Ok(report);
    }
    let Some(mut lease) = claim(store, config)? else {
        return Ok(report);
    };
    if lease.exhausted {
        finish(store, &lease, Some("capture_attempt_budget_exhausted"))?;
        report.captured = true;
        return Ok(report);
    }
    let child = cancel.child_token();
    let result = tokio::time::timeout(
        Duration::from_secs(lease.config.limits.download_seconds),
        traverse(store, &mut lease, collector, &child),
    )
    .await;
    let category = match result {
        Ok(Ok(())) => None,
        Ok(Err(_)) if cancel.is_cancelled() => Some("capture_cancelled"),
        Ok(Err(_)) => Some("capture_fetch_failed"),
        Err(_) => Some("capture_time_limit"),
    };
    child.cancel();
    finish(store, &lease, category).context("capture terminal receipt")?;
    report.captured = true;
    Ok(report)
}
pub async fn step(
    store: &Store,
    config: &Config,
    cancel: &CancellationToken,
) -> Result<CaptureTick> {
    step_with(store, config, &ConfiguredCollector, cancel).await
}
/// Supervised autonomous loop; the collector seam permits local protocol fixtures.
pub async fn run_with(
    store: Store,
    configs: Arc<tokio::sync::RwLock<crate::config::ConfigManager>>,
    collector: Arc<dyn Collector>,
    cancel: CancellationToken,
) -> Result<()> {
    while !cancel.is_cancelled() {
        let config = configs.read().await.snapshot();
        if config.daily.capture.enabled {
            match step_with(&store, &config, collector.as_ref(), &cancel).await {
                Ok(report) => {
                    if report.admitted > 0 || report.materialized > 0 || report.captured {
                        crate::operator::log(
                            "daily_capture_tick",
                            None,
                            serde_json::to_value(report)?,
                        );
                    }
                }
                Err(_) => crate::operator::log(
                    "daily_capture_error",
                    None,
                    json!({"reason":"inspect_capture_state"}),
                ),
            }
        }
        tokio::select! {_=cancel.cancelled()=>break,_=tokio::time::sleep(Duration::from_millis(200))=>{}}
    }
    Ok(())
}
