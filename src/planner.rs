//! DA02 / #69: bounded local-date admissions and durable collection lifecycle.
//! This module has no provider transport. DA03 must establish actual coverage.
use crate::{
    activities::AccountIdentity,
    config::{Config, WatchAccount},
    digest,
    storage::Store,
};
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Days, NaiveDate, TimeZone};
use chrono_tz::Tz;
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub fn account_identity(watch: &WatchAccount) -> AccountIdentity {
    match &watch.user_id {
        Some(id) => AccountIdentity::ProviderId {
            provider: watch.provider.clone(),
            user_id: id.clone(),
        },
        None => AccountIdentity::Unresolved {
            provider: watch.provider.clone(),
            key: format!("watch:{}", watch.key),
        },
    }
}
pub fn local_day_bounds(timezone: &str, date: NaiveDate) -> Result<(i64, i64)> {
    let tz: Tz = timezone.parse().context("invalid daily IANA timezone")?;
    let midnight = |day: NaiveDate| -> Result<i64> {
        Ok(tz
            .from_local_datetime(&day.and_hms_opt(0, 0, 0).context("invalid local midnight")?)
            .single()
            .context("ambiguous or nonexistent local midnight")?
            .timestamp())
    };
    let start = midnight(date)?;
    let end = midnight(date.succ_opt().context("daily date overflow")?)?;
    ensure!(end > start, "empty local day");
    Ok((start, end))
}
fn today(config: &Config, now: i64) -> Result<NaiveDate> {
    let tz: Tz = config.daily.timezone.parse()?;
    Ok(DateTime::from_timestamp(now, 0)
        .context("invalid planner clock")?
        .with_timezone(&tz)
        .date_naive())
}
fn snapshot(config: &Config) -> Result<(String, String, String)> {
    let raw = serde_json::to_string(config)?;
    let tools = serde_json::to_string(&json!({"planner":"DA02-v1"}))?;
    Ok((digest(format!("{raw}\n{tools}")), raw, tools))
}
fn save_config(tx: &Transaction<'_>, config: &Config) -> Result<String> {
    let (id, raw, tools) = snapshot(config)?;
    tx.execute(
        "INSERT OR IGNORE INTO config_snapshots VALUES(?1,?2,?3)",
        params![id, raw, tools],
    )?;
    Ok(id)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DayState {
    Planned,
    Collecting,
    Partial,
    Blocked,
    Failed,
    Complete,
    OpenCurrentDay,
}
impl DayState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Collecting => "collecting",
            Self::Partial => "partial",
            Self::Blocked => "blocked",
            Self::Failed => "failed",
            Self::Complete => "complete",
            Self::OpenCurrentDay => "open_current_day",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DayPlan {
    pub id: String,
    pub account: AccountIdentity,
    pub watch: WatchAccount,
    pub local_date: String,
    pub timezone: String,
    pub utc_start: i64,
    pub utc_end: i64,
    pub requested_from: String,
    pub requested_through: String,
    pub config_id: String,
}
fn plan(
    config: &Config,
    watch: &WatchAccount,
    date: NaiveDate,
    from: NaiveDate,
    through: NaiveDate,
    config_id: &str,
) -> Result<DayPlan> {
    let account = account_identity(watch);
    let id = digest(serde_json::to_vec(&(
        account.id()?,
        &config.daily.timezone,
        date.to_string(),
    ))?);
    let (utc_start, utc_end) = local_day_bounds(&config.daily.timezone, date)?;
    Ok(DayPlan {
        id,
        account,
        watch: watch.clone(),
        local_date: date.to_string(),
        timezone: config.daily.timezone.clone(),
        utc_start,
        utc_end,
        requested_from: from.to_string(),
        requested_through: through.to_string(),
        config_id: config_id.into(),
    })
}
#[derive(Clone, Debug, Serialize)]
pub struct BackfillReport {
    pub dry_run: bool,
    pub created: usize,
    pub existing: usize,
    pub windows: Vec<DayPlan>,
}
pub fn preview_backfill(
    config: &Config,
    from: NaiveDate,
    through: NaiveDate,
    now: i64,
) -> Result<BackfillReport> {
    config.validate()?;
    ensure!(config.daily.enabled, "daily planning is disabled");
    ensure!(
        from <= through && through <= today(config, now)?,
        "daily range must end no later than the current local day"
    );
    let days = through.signed_duration_since(from).num_days() + 1;
    ensure!(
        days <= i64::from(config.daily.max_days_per_request),
        "daily range exceeds day limit"
    );
    let watches: Vec<_> = config.daily.accounts.iter().filter(|a| a.enabled).collect();
    ensure!(
        (days as usize)
            .checked_mul(watches.len())
            .is_some_and(|n| n <= config.daily.max_backfill_windows),
        "daily range exceeds account/day window limit"
    );
    let mut windows = vec![];
    let config_id = snapshot(config)?.0;
    for watch in watches {
        let mut day = from;
        loop {
            windows.push(plan(config, watch, day, from, through, &config_id)?);
            if day == through {
                break;
            }
            day = day.succ_opt().context("daily date overflow")?;
        }
    }
    Ok(BackfillReport {
        dry_run: true,
        created: 0,
        existing: 0,
        windows,
    })
}

fn check_bindings(conn: &rusqlite::Connection, config: &Config) -> Result<()> {
    for watch in &config.daily.accounts {
        let old: Option<String> = conn
            .query_row(
                "SELECT identity FROM daily_watch_bindings WHERE watch_key=?1",
                [&watch.key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            ensure!(
                serde_json::from_str::<AccountIdentity>(&old)? == account_identity(watch),
                "daily watch key identity changed; explicit identity reconciliation is required"
            );
        }
    }
    Ok(())
}
fn bindings(tx: &Transaction<'_>, config: &Config) -> Result<()> {
    check_bindings(tx, config)?;
    tx.execute(
        "UPDATE daily_watch_bindings SET enabled=0 WHERE enabled=1",
        [],
    )?;
    for watch in &config.daily.accounts {
        let identity = account_identity(watch);
        let id = identity.id()?;
        let raw = serde_json::to_string(&identity)?;
        tx.execute(
            "INSERT OR IGNORE INTO accounts VALUES(?1,?2)",
            params![id, raw],
        )?;
        tx.execute("INSERT INTO daily_watch_bindings VALUES(?1,?2,?3,?4) ON CONFLICT(watch_key) DO UPDATE SET enabled=excluded.enabled",params![watch.key,id,raw,watch.enabled])?;
    }
    Ok(())
}
fn admit(tx: &Transaction<'_>, value: &DayPlan, now: i64) -> Result<bool> {
    Ok(tx.execute("INSERT OR IGNORE INTO daily_windows(id,account_id,timezone,local_date,utc_start,utc_end,config_id,manifest,state) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![value.id,value.account.id()?,value.timezone,value.local_date,value.utc_start,value.utc_end,value.config_id,serde_json::to_string(value)?,if value.utc_end>now {"open_current_day"}else{"planned"}])?==1)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DayWindow {
    #[serde(flatten)]
    pub plan: DayPlan,
    pub state: DayState,
    pub attempts: u32,
    pub failures: u32,
    pub generation: u64,
    pub lease_until: Option<i64>,
    pub cursor: Option<String>,
    pub reason: Option<String>,
    pub open_current_day: bool,
}
fn window(conn: &rusqlite::Connection, id: &str, now: i64) -> Result<DayWindow> {
    type WindowRow = (
        String,
        String,
        u32,
        u32,
        u64,
        Option<i64>,
        Option<String>,
        Option<String>,
    );
    let (raw,state,attempts,failures,generation,lease_until,cursor,reason):WindowRow=conn.query_row("SELECT manifest,state,attempts,failures,generation,lease_until,cursor,reason FROM daily_windows WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)))?;
    let plan: DayPlan = serde_json::from_str(&raw)?;
    let mut state: DayState = serde_json::from_value(json!(state))?;
    let open_current_day = plan.utc_start <= now && now < plan.utc_end;
    if state == DayState::OpenCurrentDay && !open_current_day {
        state = DayState::Planned;
    }
    Ok(DayWindow {
        plan,
        state,
        attempts,
        failures,
        generation,
        lease_until,
        cursor,
        reason,
        open_current_day,
    })
}
#[derive(Clone, Debug, Default, Serialize)]
pub struct DailyTick {
    pub created: usize,
    pub visited: usize,
    pub blocked: usize,
    pub paused: bool,
}

impl Store {
    pub fn validate_daily_config(&self, config: &Config) -> Result<()> {
        config.validate()?;
        let conn = self.connection()?;
        check_bindings(&conn, config)
    }
    pub fn backfill_daily(
        &self,
        config: &Config,
        from: NaiveDate,
        through: NaiveDate,
        now: i64,
    ) -> Result<BackfillReport> {
        let mut report = preview_backfill(config, from, through, now)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        bindings(&tx, config)?;
        save_config(&tx, config)?;
        for value in &mut report.windows {
            if admit(&tx, value, now)? {
                report.created += 1;
            } else {
                report.existing += 1;
                *value = window(&tx, &value.id, now)?.plan;
            }
        }
        tx.commit()?;
        report.dry_run = false;
        Ok(report)
    }
    pub fn daily_window(&self, id: &str, now: i64) -> Result<DayWindow> {
        let conn = self.connection()?;
        window(&conn, id, now)
    }
    pub fn daily_status(
        &self,
        limit: usize,
        after: Option<&str>,
        now: i64,
    ) -> Result<Vec<DayWindow>> {
        ensure!(
            (1..=1000).contains(&limit),
            "daily status limit must be 1..1000"
        );
        let conn = self.connection()?;
        let ids = conn
            .prepare("SELECT id FROM daily_windows WHERE id>?1 ORDER BY id LIMIT ?2")?
            .query_map(params![after.unwrap_or(""), limit], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        ids.iter().map(|id| window(&conn, id, now)).collect()
    }
    /// Explicit operator retry; retains the cursor and immutable attempt/page history.
    pub fn retry_daily_window(&self, config: &Config, id: &str, now: i64) -> Result<DayWindow> {
        config.validate()?;
        ensure!(config.daily.enabled, "daily planning is disabled");
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        bindings(&tx, config)?;
        let value = window(&tx, id, now)?;
        ensure!(
            matches!(value.state, DayState::Failed | DayState::Blocked),
            "daily retry requires a failed or blocked window"
        );
        ensure!(
            config
                .daily
                .accounts
                .iter()
                .filter(|a| a.enabled)
                .any(|a| account_identity(a).id().ok() == value.plan.account.id().ok()),
            "daily retry requires an enabled watch"
        );
        tx.execute("UPDATE daily_windows SET state=?2,failures=0,generation=generation+1,lease_until=NULL,reason='operator_retry',next_attempt_at=?3 WHERE id=?1", params![id,if value.open_current_day {"open_current_day"} else {"planned"},now])?;
        let value = window(&tx, id, now)?;
        tx.commit()?;
        Ok(value)
    }
    /// Actual periodic runtime entry point. Provider absence is an explicit block,
    /// never empty-result coverage or a successful collection attempt.
    pub fn daily_tick(&self, config: &Config, now: i64) -> Result<DailyTick> {
        config.validate()?;
        let mut result = DailyTick::default();
        if !config.daily.enabled {
            return Ok(result);
        }
        if self.recovery_paused()? {
            result.paused = true;
            return Ok(result);
        }
        let current = today(config, now)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        bindings(&tx, config)?;
        let config_id = save_config(&tx, config)?;
        let mut watches: Vec<_> = config
            .daily
            .accounts
            .iter()
            .filter(|a| a.enabled)
            .map(|a| Ok((account_identity(a).id()?, a)))
            .collect::<Result<_>>()?;
        watches.sort_by(|a, b| a.0.cmp(&b.0));
        let last: Option<String> = tx.query_row(
            "SELECT last_account FROM daily_scheduler WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        if let Some(last) = last {
            let offset = watches.partition_point(|(id, _)| id <= &last);
            let len = watches.len();
            if len > 0 {
                watches.rotate_left(offset % len);
            }
        }
        for (id, watch) in watches {
            if result.visited >= config.daily.max_windows_per_tick {
                break;
            }
            let cursors:Option<(String,Option<String>)>=tx.query_row("SELECT next_date,reconcile_date FROM daily_admission_cursors WHERE account_id=?1 AND timezone=?2",params![id,config.daily.timezone],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
            let (mut day, reconcile_date) = match cursors {
                Some((s, r)) => (s.parse::<NaiveDate>()?, r),
                None => (
                    current
                        .checked_sub_days(Days::new(u64::from(
                            config.daily.first_run_lookback_days - 1,
                        )))
                        .context("daily lookback overflow")?,
                    None,
                ),
            };
            let from = day;
            while day <= current && result.visited < config.daily.max_windows_per_tick {
                result.created += usize::from(admit(
                    &tx,
                    &plan(config, watch, day, from, current, &config_id)?,
                    now,
                )?);
                result.visited += 1;
                day = day.succ_opt().context("daily date overflow")?;
                tx.execute("INSERT INTO daily_admission_cursors(account_id,timezone,next_date) VALUES(?1,?2,?3) ON CONFLICT(account_id,timezone) DO UPDATE SET next_date=excluded.next_date",params![id,config.daily.timezone,day.to_string()])?;
            }
            if day > current && config.daily.reconciliation_days > 0 {
                let first_recent = current
                    .checked_sub_days(Days::new(u64::from(config.daily.reconciliation_days - 1)))
                    .context("daily reconciliation overflow")?;
                let mut recent = reconcile_date
                    .map(|s| s.parse::<NaiveDate>())
                    .transpose()?
                    .filter(|d| *d >= first_recent && *d <= current)
                    .unwrap_or(first_recent);
                while recent <= current && result.visited < config.daily.max_windows_per_tick {
                    let value = plan(config, watch, recent, recent, current, &config_id)?;
                    result.created += usize::from(admit(&tx, &value, now)?);
                    result.visited += 1;
                    tx.execute("UPDATE daily_windows SET state='planned',cursor=NULL,reason='reconciliation_due',next_attempt_at=?2 WHERE id=?1 AND state='complete' AND next_attempt_at<=?2",params![value.id,now])?;
                    recent = recent.succ_opt().context("daily date overflow")?;
                    tx.execute("UPDATE daily_admission_cursors SET reconcile_date=?3 WHERE account_id=?1 AND timezone=?2",params![id,config.daily.timezone,if recent>current {first_recent}else{recent}.to_string()])?;
                }
            }
            tx.execute(
                "UPDATE daily_scheduler SET last_account=?1 WHERE singleton=1",
                [id],
            )?;
        }
        let ids=tx.prepare("SELECT id FROM daily_windows WHERE (state IN ('planned','partial') OR (state='collecting' AND lease_until<=?1) OR (state='open_current_day' AND (utc_end<=?1 OR reason IS NULL OR reason!='timeline_provider_not_installed'))) AND account_id IN (SELECT account_id FROM daily_watch_bindings WHERE enabled=1) ORDER BY utc_start,id LIMIT ?2")?.query_map(params![now,config.daily.max_windows_per_tick],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        for id in ids {
            let value = window(&tx, &id, now)?;
            if value.state == DayState::Collecting {
                tx.execute("UPDATE daily_attempts SET state='interrupted',finished_at=?2 WHERE window_id=?1 AND state='collecting'",params![id,now])?;
                tx.execute("UPDATE daily_windows SET failures=failures+1,generation=generation+1 WHERE id=?1",[&id])?;
            }
            tx.execute("UPDATE daily_windows SET state=?2,lease_until=NULL,reason='timeline_provider_not_installed' WHERE id=?1",params![id,if value.open_current_day {"open_current_day"}else{"blocked"}])?;
            result.blocked += 1;
        }
        tx.commit()?;
        Ok(result)
    }
}

#[derive(Clone, Debug)]
pub struct DailyLease {
    pub window: DayWindow,
    pub config: Config,
    pub watch: WatchAccount,
    pub config_id: String,
    pub resolved_account: Option<AccountIdentity>,
}
#[derive(Clone, Debug)]
pub struct DailyProgress {
    pub state: DayState,
    pub cursor: Option<String>,
    pub reason: Option<String>,
    pub evidence: Value,
    /// Only a collection adapter with actual coverage evidence may set this.
    pub coverage_complete: bool,
}
fn fence(tx: &Transaction<'_>, lease: &DailyLease, now: i64) -> Result<()> {
    let active:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM daily_windows WHERE id=?1 AND state='collecting' AND generation=?2 AND attempts=?3 AND lease_until>?4)",params![lease.window.plan.id,lease.window.generation,lease.window.attempts,now],|r|r.get(0))?;
    ensure!(active, "stale daily collection lease");
    let (manifest,watch):(String,String)=tx.query_row("SELECT w.manifest,a.watch FROM daily_windows w JOIN daily_attempts a ON a.window_id=w.id AND a.attempt=w.attempts WHERE w.id=?1",[&lease.window.plan.id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    ensure!(
        serde_json::from_str::<Value>(&manifest)? == serde_json::to_value(&lease.window.plan)?
            && serde_json::from_str::<Value>(&watch)? == serde_json::to_value(&lease.watch)?,
        "daily lease watch/window snapshot changed"
    );
    let (config,tools):(String,String)=tx.query_row("SELECT c.config,c.tools FROM daily_attempts a JOIN config_snapshots c ON c.id=a.config_id WHERE a.window_id=?1 AND a.attempt=?2 AND a.config_id=?3",params![lease.window.plan.id,lease.window.attempts,lease.config_id],|r|Ok((r.get(0)?,r.get(1)?)))?;
    ensure!(
        digest(format!("{config}\n{tools}")) == lease.config_id
            && serde_json::to_value(serde_json::from_str::<Config>(&config)?)?
                == serde_json::to_value(&lease.config)?,
        "daily lease snapshot changed"
    );
    Ok(())
}
impl Store {
    /// Used by DA03 after provider readiness; DA02's dispatcher never invokes it.
    pub fn claim_daily_window(&self, config: &Config, now: i64) -> Result<Option<DailyLease>> {
        config.validate()?;
        if !config.daily.enabled || self.recovery_paused()? {
            return Ok(None);
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        bindings(&tx, config)?;
        let ids=tx.prepare("SELECT id FROM daily_windows WHERE state!='complete' AND failures<?3 AND next_attempt_at<=?1 AND (state!='collecting' OR lease_until<=?1) AND account_id IN (SELECT account_id FROM daily_watch_bindings WHERE enabled=1) ORDER BY utc_start,id LIMIT ?2")?.query_map(params![now,config.daily.max_windows_per_tick,config.daily.max_attempts],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        for id in ids {
            let old = window(&tx, &id, now)?;
            let failures = old.failures + u32::from(old.state == DayState::Collecting);
            if old.state == DayState::Collecting {
                tx.execute("UPDATE daily_attempts SET state='interrupted',finished_at=?2 WHERE window_id=?1 AND state='collecting'",params![id,now])?;
            }
            if failures >= config.daily.max_attempts {
                tx.execute("UPDATE daily_windows SET state='failed',failures=?2,reason='attempt_budget_exhausted',lease_until=NULL,generation=generation+1 WHERE id=?1",params![id,failures])?;
                continue;
            }
            let config_id = save_config(&tx, config)?;
            let watch = config
                .daily
                .accounts
                .iter()
                .find(|w| {
                    w.enabled
                        && account_identity(w).id().ok().as_ref()
                            == Some(&old.plan.account.id().unwrap_or_default())
                })
                .context("enabled daily account vanished")?
                .clone();
            tx.execute("UPDATE daily_windows SET state='collecting',attempts=attempts+1,failures=?2,generation=generation+1,lease_until=?3,reason=NULL WHERE id=?1",params![id,failures,now.saturating_add(config.limits.lease_seconds as i64)])?;
            let claimed = window(&tx, &id, now)?;
            tx.execute("INSERT INTO daily_attempts(window_id,attempt,generation,config_id,watch,started_at,state,cursor,evidence) VALUES(?1,?2,?3,?4,?5,?6,'collecting',?7,'{}')",params![id,claimed.attempts,claimed.generation,config_id,serde_json::to_string(&watch)?,now,claimed.cursor])?;
            let resolved_account = match &watch.user_id {
                Some(_) => Some(account_identity(&watch)),
                None => tx
                    .query_row(
                        "SELECT identity FROM daily_account_resolutions WHERE watch_key=?1",
                        [&watch.key],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()?
                    .map(|s| serde_json::from_str(&s))
                    .transpose()?,
            };
            tx.commit()?;
            return Ok(Some(DailyLease {
                window: claimed,
                config: config.clone(),
                watch,
                config_id,
                resolved_account,
            }));
        }
        tx.commit()?;
        Ok(None)
    }
    pub fn checkpoint_daily(
        &self,
        lease: &DailyLease,
        cursor: &str,
        evidence: &Value,
        now: i64,
    ) -> Result<()> {
        ensure!(
            !cursor.is_empty() && cursor.len() <= 65536,
            "invalid daily cursor"
        );
        let raw = serde_json::to_string(evidence)?;
        ensure!(
            raw.len() as u64 <= lease.config.limits.input_bytes.min(1_048_576),
            "daily evidence exceeds limit"
        );
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        fence(&tx, lease, now)?;
        let (prior, pages): (Option<String>, usize) = tx.query_row(
            "SELECT cursor,pages FROM daily_attempts WHERE window_id=?1 AND attempt=?2",
            params![lease.window.plan.id, lease.window.attempts],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        ensure!(prior.as_deref() != Some(cursor), "daily cursor stalled");
        ensure!(
            pages < lease.config.daily.provider.max_pages,
            "daily page budget exhausted"
        );
        let repeated:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM daily_pages WHERE window_id=?1 AND attempt=?2 AND cursor=?3)",params![lease.window.plan.id,lease.window.attempts,cursor],|r|r.get(0))?;
        ensure!(!repeated, "daily cursor cycle");
        tx.execute(
            "INSERT INTO daily_pages VALUES(?1,?2,?3,?4,?5)",
            params![
                lease.window.plan.id,
                lease.window.attempts,
                pages + 1,
                cursor,
                raw
            ],
        )?;
        tx.execute("UPDATE daily_attempts SET cursor=?3,pages=pages+1,evidence=?4 WHERE window_id=?1 AND attempt=?2",params![lease.window.plan.id,lease.window.attempts,cursor,raw])?;
        tx.execute(
            "UPDATE daily_windows SET cursor=?2,lease_until=?3 WHERE id=?1",
            params![
                lease.window.plan.id,
                cursor,
                now.saturating_add(lease.config.limits.lease_seconds as i64)
            ],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn bind_daily_account(
        &self,
        lease: &DailyLease,
        user_id: &str,
        evidence: &Value,
        now: i64,
    ) -> Result<AccountIdentity> {
        ensure!(
            !user_id.is_empty()
                && user_id.len() <= 30
                && user_id.bytes().all(|b| b.is_ascii_digit()),
            "invalid daily provider user ID"
        );
        ensure!(
            lease.watch.user_id.as_ref().is_none_or(|id| id == user_id),
            "configured daily provider user ID mismatch"
        );
        let identity = AccountIdentity::ProviderId {
            provider: lease.watch.provider.clone(),
            user_id: user_id.into(),
        };
        let raw = serde_json::to_string(&identity)?;
        let evidence = serde_json::to_string(evidence)?;
        ensure!(
            evidence.len() as u64 <= lease.config.limits.input_bytes.min(1_048_576),
            "daily identity evidence exceeds limit"
        );
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        fence(&tx, lease, now)?;
        let prior: Option<String> = tx
            .query_row(
                "SELECT identity FROM daily_account_resolutions WHERE watch_key=?1",
                [&lease.watch.key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(prior) = prior {
            ensure!(
                serde_json::from_str::<AccountIdentity>(&prior)? == identity,
                "daily resolved identity changed; explicit reconciliation is required"
            );
        }
        let duplicate:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM daily_account_resolutions r JOIN daily_watch_bindings b USING(watch_key) WHERE r.account_id=?1 AND r.watch_key!=?2 AND b.enabled=1)",params![identity.id()?,lease.watch.key],|r|r.get(0))?;
        ensure!(
            !duplicate,
            "duplicate resolved daily account requires reconciliation"
        );
        tx.execute(
            "INSERT OR IGNORE INTO accounts VALUES(?1,?2)",
            params![identity.id()?, raw],
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO daily_account_resolutions VALUES(?1,?2,?3,?4,?5)",
            params![lease.watch.key, identity.id()?, raw, now, evidence],
        )?;
        tx.commit()?;
        Ok(identity)
    }
    pub fn finish_daily_window(
        &self,
        lease: &DailyLease,
        progress: DailyProgress,
        now: i64,
    ) -> Result<()> {
        ensure!(
            matches!(
                progress.state,
                DayState::Partial
                    | DayState::Blocked
                    | DayState::Failed
                    | DayState::Complete
                    | DayState::OpenCurrentDay
            ),
            "invalid daily terminal attempt state"
        );
        ensure!(
            progress
                .cursor
                .as_ref()
                .is_none_or(|s| !s.is_empty() && s.len() <= 65536),
            "invalid daily cursor"
        );
        ensure!(
            progress.reason.as_ref().is_none_or(|s| !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))),
            "daily reason must be a safe category"
        );
        let evidence = serde_json::to_string(&progress.evidence)?;
        ensure!(
            evidence.len() as u64 <= lease.config.limits.input_bytes.min(1_048_576),
            "daily evidence exceeds limit"
        );
        let mut state = progress.state;
        let mut reason = progress.reason;
        if state == DayState::Complete {
            ensure!(
                progress.coverage_complete && progress.cursor.is_none(),
                "daily completion requires exhausted cursor and actual coverage evidence"
            );
            if now < lease.window.plan.utc_end {
                state = DayState::OpenCurrentDay;
                reason = Some("window_still_open".into());
            }
        }
        if state == DayState::Partial
            && progress.cursor.is_some()
            && progress.cursor == lease.window.cursor
        {
            state = DayState::Blocked;
            reason = Some("cursor_stalled".into());
        }
        let failed = matches!(state, DayState::Blocked | DayState::Failed);
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        fence(&tx, lease, now)?;
        let failures: u32 = tx.query_row(
            "SELECT failures FROM daily_windows WHERE id=?1",
            [&lease.window.plan.id],
            |r| r.get(0),
        )?;
        if failed && failures.saturating_add(1) >= lease.config.daily.max_attempts {
            state = DayState::Failed;
            reason = Some("attempt_budget_exhausted".into());
        }
        tx.execute("UPDATE daily_attempts SET state=?3,finished_at=?4,cursor=?5,evidence=?6 WHERE window_id=?1 AND attempt=?2",params![lease.window.plan.id,lease.window.attempts,state.as_str(),now,progress.cursor,evidence])?;
        tx.execute("UPDATE daily_windows SET state=?2,cursor=?3,reason=?4,lease_until=NULL,failures=CASE WHEN ?5 THEN failures+1 ELSE 0 END,next_attempt_at=?6 WHERE id=?1",params![lease.window.plan.id,state.as_str(),progress.cursor,reason,failed,now.saturating_add(lease.config.daily.interval_seconds as i64)])?;
        tx.commit()?;
        Ok(())
    }
}
