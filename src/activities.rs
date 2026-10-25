//! DA01 / #68: account activities reference the canonical source/revision store.
//!
//! Provider adapters supply identities and evidence; this module performs no I/O
//! outside SQLite and the existing local capture/view operations.
use crate::{
    digest,
    sources::{Capture, FetchStatus, Identity, ViewPolicy},
    storage::Store,
};
use anyhow::{Context, Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "identity", rename_all = "snake_case", deny_unknown_fields)]
pub enum AccountIdentity {
    ProviderId {
        provider: String,
        user_id: String,
    },
    /// A caller-scoped uncertainty key, never a handle inferred to be stable.
    Unresolved {
        provider: String,
        key: String,
    },
}
impl AccountIdentity {
    pub fn provider(&self) -> &str {
        match self {
            Self::ProviderId { provider, .. } | Self::Unresolved { provider, .. } => provider,
        }
    }
    pub fn id(&self) -> Result<String> {
        nonempty(self.provider())?;
        match self {
            Self::ProviderId { user_id, .. } => nonempty(user_id)?,
            Self::Unresolved { key, .. } => nonempty(key)?,
        }
        Ok(digest(serde_json::to_vec(self)?))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimePrecision {
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Timestamp {
    pub seconds: i64,
    pub nanoseconds: u32,
}
impl Timestamp {
    pub fn seconds(seconds: i64) -> Self {
        Self {
            seconds,
            nanoseconds: 0,
        }
    }
    fn validate(self) -> Result<()> {
        ensure!(
            self.nanoseconds < 1_000_000_000,
            "invalid timestamp nanoseconds"
        );
        Ok(())
    }
}

/// UTC timestamps with the provider's precision and its original evidence.
/// Intervals are inclusive; unknown is never substituted with source time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "certainty", rename_all = "snake_case", deny_unknown_fields)]
pub enum TimeEvidence {
    Unknown {
        provenance: Value,
    },
    Instant {
        timestamp: Timestamp,
        precision: TimePrecision,
        provenance: Value,
    },
    Interval {
        earliest: Timestamp,
        latest: Timestamp,
        precision: TimePrecision,
        provenance: Value,
    },
}
impl TimeEvidence {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Unknown { .. } => {}
            Self::Instant { timestamp, .. } => timestamp.validate()?,
            Self::Interval {
                earliest, latest, ..
            } => {
                earliest.validate()?;
                latest.validate()?;
                ensure!(earliest <= latest, "activity time interval is reversed");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryRun {
    pub provider: String,
    /// Stable across retries of one discovery run; a new run gets a new key.
    pub key: String,
    pub provenance: Value,
}
impl DiscoveryRun {
    pub fn id(&self) -> Result<String> {
        nonempty(&self.provider)?;
        nonempty(&self.key)?;
        Ok(digest(serde_json::to_vec(&(&self.provider, &self.key))?))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityKind {
    Original,
    Quote,
    Reply,
    Repost,
}
impl ActivityKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Quote => "quote",
            Self::Reply => "reply",
            Self::Repost => "repost",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "identity", rename_all = "snake_case", deny_unknown_fields)]
pub enum ActivityIdentity {
    ProviderId {
        provider: String,
        event_id: String,
    },
    /// Actor/source scoped; no cross-run deduplication is claimed without an ID.
    Unresolved {
        provider: String,
        discovery_run_id: String,
        occurrence_key: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Activity {
    pub identity: ActivityIdentity,
    pub actor: AccountIdentity,
    pub kind: ActivityKind,
    /// For original/quote/reply this is its post; for repost it is the target.
    /// Quote/reply context uses the existing typed source relations.
    pub source: Identity,
}
impl Activity {
    pub fn id(&self) -> Result<String> {
        self.actor.id()?;
        nonempty(&self.source.provider)?;
        nonempty(&self.source.external_id)?;
        match &self.identity {
            ActivityIdentity::ProviderId { provider, event_id } => {
                ensure!(
                    provider == self.actor.provider(),
                    "activity provider mismatch"
                );
                nonempty(event_id)?;
            }
            ActivityIdentity::Unresolved {
                provider,
                discovery_run_id,
                occurrence_key,
            } => {
                ensure!(
                    provider == self.actor.provider(),
                    "activity provider mismatch"
                );
                nonempty(discovery_run_id)?;
                nonempty(occurrence_key)?;
            }
        }
        // Provider IDs are authoritative; ambiguous page-position keys also
        // need actor/source scope to preserve demonstrably distinct events.
        let key = match &self.identity {
            ActivityIdentity::ProviderId { .. } => serde_json::to_vec(&self.identity)?,
            ActivityIdentity::Unresolved { .. } => {
                serde_json::to_vec(&(&self.identity, self.actor.id()?, self.source.id()))?
            }
        };
        Ok(digest(key))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountObservation {
    pub account: AccountIdentity,
    pub handle: Option<String>,
    pub display_name: Option<String>,
    pub retrieved_at: i64,
    pub provenance: Value,
}
impl AccountObservation {
    pub fn id(&self) -> Result<String> {
        self.account.id()?;
        Ok(digest(serde_json::to_vec(self)?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityCapture {
    pub actor: AccountIdentity,
    pub handle: Option<String>,
    pub display_name: Option<String>,
    pub provider_event_id: Option<String>,
    pub kind: ActivityKind,
    pub discovery: DiscoveryRun,
    /// Distinct occurrences need distinct keys, even for the same actor/target.
    pub occurrence_key: String,
    /// Reuse only for identical retries; fresh fetches/hydration need a new key.
    pub observation_key: String,
    pub activity_time: TimeEvidence,
    pub source_time: TimeEvidence,
    pub retrieved_at: i64,
    pub provenance: Value,
    pub capture: Capture,
    pub view_policy: ViewPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivityObservation {
    pub schema_version: u32,
    pub id: String,
    pub activity: Activity,
    pub account_observation: AccountObservation,
    pub discovery: DiscoveryRun,
    pub observation_key: String,
    pub input_digest: String,
    pub source_observation_id: i64,
    pub view_id: String,
    pub revision_id: Option<String>,
    pub missing_reason: Option<String>,
    pub activity_time: TimeEvidence,
    pub source_time: TimeEvidence,
    pub retrieved_at: i64,
    pub fetch_status: FetchStatus,
    pub provenance: Value,
}

fn nonempty(value: &str) -> Result<()> {
    ensure!(
        !value.trim().is_empty(),
        "empty account/activity identity key"
    );
    Ok(())
}
fn observation_id(activity: &Activity, discovery: &DiscoveryRun, key: &str) -> Result<String> {
    nonempty(key)?;
    Ok(digest(serde_json::to_vec(&(
        activity.id()?,
        discovery.id()?,
        key,
    ))?))
}

/// Only internal, fixed table names reach this helper.
fn immutable(tx: &Transaction<'_>, table: &str, id: &str, manifest: &str) -> Result<bool> {
    let old: Option<String> = tx
        .query_row(
            &format!("SELECT manifest FROM {table} WHERE id=?1"),
            [id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(old) = old {
        ensure!(old == manifest, "conflicting immutable {table} identity");
        Ok(false)
    } else {
        Ok(true)
    }
}

/// Shared by native ingestion and semantic restore. No source body is copied.
pub(crate) fn save_observation(tx: &Transaction<'_>, value: &ActivityObservation) -> Result<()> {
    ensure!(
        value.schema_version == 1,
        "unsupported activity manifest schema"
    );
    ensure!(
        value.id == observation_id(&value.activity, &value.discovery, &value.observation_key)?,
        "activity observation identity mismatch"
    );
    value.activity_time.validate()?;
    value.source_time.validate()?;
    ensure!(
        value.activity.actor == value.account_observation.account,
        "activity account mismatch"
    );
    ensure!(
        value.activity.actor.provider() == value.discovery.provider,
        "discovery provider mismatch"
    );
    if let ActivityIdentity::Unresolved {
        discovery_run_id, ..
    } = &value.activity.identity
    {
        ensure!(
            *discovery_run_id == value.discovery.id()?,
            "uncertain activity discovery mismatch"
        );
    }
    ensure!(
        value.input_digest.len() == 64 && value.input_digest.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid activity input digest"
    );
    let source = value.activity.source.id();
    let (observed_source, status): (String, String) = tx.query_row(
        "SELECT source_id,status FROM observations WHERE id=?1",
        [value.source_observation_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    ensure!(
        observed_source == source && serde_json::to_value(value.fetch_status)? == status,
        "activity source observation mismatch"
    );
    let (root, manifest): (String, String) = tx.query_row(
        "SELECT root_id,manifest FROM views WHERE id=?1",
        [&value.view_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let view: crate::sources::View = serde_json::from_str(&manifest)?;
    ensure!(
        view.id()? == value.view_id && root == source,
        "activity view mismatch"
    );
    let member = view
        .members
        .iter()
        .find(|m| m.source_id == source)
        .context("activity view root missing")?;
    ensure!(
        member.revision_id == value.revision_id && member.missing_reason == value.missing_reason,
        "activity revision pin mismatch"
    );
    let actor = value.activity.actor.id()?;
    let account = serde_json::to_string(&value.activity.actor)?;
    if immutable(tx, "accounts", &actor, &account)? {
        tx.execute(
            "INSERT INTO accounts VALUES(?1,?2)",
            params![actor, account],
        )?;
    }
    let alias_id = value.account_observation.id()?;
    let alias = serde_json::to_string(&value.account_observation)?;
    if immutable(tx, "account_observations", &alias_id, &alias)? {
        tx.execute(
            "INSERT INTO account_observations VALUES(?1,?2,?3)",
            params![alias_id, actor, alias],
        )?;
    }
    let run_id = value.discovery.id()?;
    let run = serde_json::to_string(&value.discovery)?;
    if immutable(tx, "discovery_runs", &run_id, &run)? {
        tx.execute(
            "INSERT INTO discovery_runs VALUES(?1,?2)",
            params![run_id, run],
        )?;
    }
    let activity_id = value.activity.id()?;
    let activity = serde_json::to_string(&value.activity)?;
    if immutable(tx, "activities", &activity_id, &activity)? {
        tx.execute(
            "INSERT INTO activities VALUES(?1,?2,?3,?4,?5)",
            params![
                activity_id,
                actor,
                source,
                value.activity.kind.as_str(),
                activity
            ],
        )?;
    }
    let manifest = serde_json::to_string(value)?;
    if immutable(tx, "activity_observations", &value.id, &manifest)? {
        tx.execute(
            "INSERT INTO activity_observations VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                value.id,
                activity_id,
                source,
                value.source_observation_id,
                alias_id,
                run_id,
                value.view_id,
                manifest
            ],
        )?;
    }
    tx.execute(
        "INSERT OR IGNORE INTO projection_events(kind,entity_id) VALUES('activity_observation',?1)",
        [&value.id],
    )?;
    Ok(())
}

impl Store {
    /// One writer transaction makes source capture, aliases, activity evidence and
    /// its revision-pinned view converge across identical retries and workers.
    pub fn record_activity(&self, input: &ActivityCapture) -> Result<ActivityObservation> {
        input.activity_time.validate()?;
        input.source_time.validate()?;
        let run_id = input.discovery.id()?;
        let activity = Activity {
            identity: match &input.provider_event_id {
                Some(event_id) => ActivityIdentity::ProviderId {
                    provider: input.actor.provider().into(),
                    event_id: event_id.clone(),
                },
                None => ActivityIdentity::Unresolved {
                    provider: input.actor.provider().into(),
                    discovery_run_id: run_id,
                    occurrence_key: input.occurrence_key.clone(),
                },
            },
            actor: input.actor.clone(),
            kind: input.kind,
            source: input.capture.identity.clone(),
        };
        let id = observation_id(&activity, &input.discovery, &input.observation_key)?;
        let input_digest = digest(serde_json::to_vec(input)?);
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let prior: Option<String> = tx
            .query_row(
                "SELECT manifest FROM activity_observations WHERE id=?1",
                [&id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(prior) = prior {
            let prior: ActivityObservation = serde_json::from_str(&prior)?;
            ensure!(
                prior.input_digest == input_digest,
                "activity observation key reused with different input"
            );
            tx.commit()?;
            return Ok(prior);
        }
        let first_event: i64 = tx.query_row(
            "SELECT COALESCE(MAX(id),0) FROM projection_events",
            [],
            |r| r.get(0),
        )?;
        self.capture_in_tx(&tx, &input.capture)?;
        let source_observation_id = tx.query_row(
            "SELECT MAX(id) FROM observations WHERE source_id=?1",
            [activity.source.id()],
            |r| r.get(0),
        )?;
        let view = self.create_view_in_tx(&tx, &activity.source, input.view_policy.clone())?;
        let member = &view.members[0];
        let observation = ActivityObservation {
            schema_version: 1,
            id,
            activity,
            account_observation: AccountObservation {
                account: input.actor.clone(),
                handle: input.handle.clone(),
                display_name: input.display_name.clone(),
                retrieved_at: input.retrieved_at,
                provenance: input.provenance.clone(),
            },
            discovery: input.discovery.clone(),
            observation_key: input.observation_key.clone(),
            input_digest,
            source_observation_id,
            view_id: view.id()?,
            revision_id: member.revision_id.clone(),
            missing_reason: member.missing_reason.clone(),
            activity_time: input.activity_time.clone(),
            source_time: input.source_time.clone(),
            retrieved_at: input.retrieved_at,
            fetch_status: input.capture.status,
            provenance: input.provenance.clone(),
        };
        save_observation(&tx, &observation)?;
        tx.execute(
            "UPDATE projection_events SET group_key=?1 WHERE id>?2",
            params![format!("activity:{}", observation.id), first_event],
        )?;
        tx.commit()?;
        Ok(observation)
    }

    pub fn activity(&self, id: &str) -> Result<Activity> {
        let manifest: String = self.connection()?.query_row(
            "SELECT manifest FROM activities WHERE id=?1",
            [id],
            |r| r.get(0),
        )?;
        Ok(serde_json::from_str(&manifest)?)
    }
    pub fn activity_observation(&self, id: &str) -> Result<ActivityObservation> {
        let manifest: String = self.connection()?.query_row(
            "SELECT manifest FROM activity_observations WHERE id=?1",
            [id],
            |r| r.get(0),
        )?;
        Ok(serde_json::from_str(&manifest)?)
    }
    pub fn activity_observations(&self, activity_id: &str) -> Result<Vec<ActivityObservation>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(
            "SELECT manifest FROM activity_observations WHERE activity_id=?1 ORDER BY id",
        )?;
        stmt.query_map([activity_id], |r| r.get::<_, String>(0))?
            .map(|s| Ok(serde_json::from_str(&s?)?))
            .collect()
    }
    pub fn account_activities(&self, account_id: &str) -> Result<Vec<Activity>> {
        let conn = self.connection()?;
        let mut stmt =
            conn.prepare("SELECT manifest FROM activities WHERE actor_id=?1 ORDER BY id")?;
        stmt.query_map([account_id], |r| r.get::<_, String>(0))?
            .map(|s| Ok(serde_json::from_str(&s?)?))
            .collect()
    }
    pub fn account_history(&self, account_id: &str) -> Result<Vec<AccountObservation>> {
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare("SELECT manifest FROM account_observations WHERE account_id=?1 ORDER BY id")?;
        stmt.query_map([account_id], |r| r.get::<_, String>(0))?
            .map(|s| Ok(serde_json::from_str(&s?)?))
            .collect()
    }
    /// Accounts, immutable activities, and discovery observations respectively.
    pub fn activity_counts(&self) -> Result<(u64, u64, u64)> {
        Ok(self.connection()?.query_row("SELECT (SELECT COUNT(*) FROM accounts),(SELECT COUNT(*) FROM activities),(SELECT COUNT(*) FROM activity_observations)", [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?)
    }
}
