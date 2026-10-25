//! DA08 / #75. One private-store lock serializes dispatch; committed intents survive process death.
use crate::{
    config::{ConfigManager, FailureReporting},
    digest, failure_reporting,
    process::{Tool, ToolFailure},
    storage::{Store, now},
};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

pub(crate) async fn serve(
    store: Store,
    config: Arc<RwLock<ConfigManager>>,
    shutdown: CancellationToken,
) -> Result<()> {
    while !shutdown.is_cancelled() {
        let (snapshot, changed) = config.read().await.reporting_snapshot();
        let cancel = shutdown.child_token();
        let operation = tick(&store, &snapshot.failure_reporting, now(), &cancel);
        tokio::pin!(operation);
        let result = tokio::select! {
            biased;
            _ = shutdown.cancelled() => { cancel.cancel(); operation.await },
            _ = changed.cancelled() => { cancel.cancel(); operation.await },
            result = &mut operation => result,
        };
        if result.is_err() {
            // Reporting must never recursively submit its own failures or expose gh diagnostics.
            crate::operator::log(
                "failure_report_worker_error",
                None,
                json!({"reason":"private-report-state"}),
            );
        }
        tokio::select! { _ = shutdown.cancelled() => break, _ = changed.cancelled() => {}, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
    }
    Ok(())
}

#[derive(Debug)]
struct Report {
    fingerprint: String,
    issue: Option<u64>,
    action: Option<String>,
    body: Option<String>,
    pending_sequence: Option<i64>,
    uncertain: bool,
    attempt: i64,
    sequence: i64,
    metadata: Value,
    occurrences: i64,
    first: i64,
    last: i64,
    state: String,
    eligible: bool,
}
impl Store {
    fn report_lock(&self) -> Result<Option<File>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(self.root().join("failure-report.lock"))?;
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(Some(file)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    fn bind_reports(&self, config: &FailureReporting) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT OR IGNORE INTO failure_report_repositories(repository) VALUES(?1)",
            [&config.repository],
        )?;
        let cursor: i64 = tx.query_row(
            "SELECT cursor FROM failure_report_repositories WHERE repository=?1",
            [&config.repository],
            |r| r.get(0),
        )?;
        let restored: i64 = tx.query_row(
            "SELECT restore_through FROM failure_reporting_state",
            [],
            |r| r.get(0),
        )?;
        let rows = tx.prepare("SELECT id,fingerprint,actionable FROM failure_incidents WHERE id>?1 ORDER BY id LIMIT 64")?.query_map([cursor],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,bool>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
        for (id, fingerprint, actionable) in &rows {
            if *actionable {
                tx.execute("INSERT OR IGNORE INTO failure_reports(repository,fingerprint,state,uncertain) VALUES(?1,?2,?3,?4)",params![config.repository,fingerprint,if *id<=restored {"reconcile"} else {"waiting_threshold"},*id<=restored])?;
            }
        }
        if let Some((last, _, _)) = rows.last() {
            tx.execute(
                "UPDATE failure_report_repositories SET cursor=?1 WHERE repository=?2",
                params![last, config.repository],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    fn report_next(&self, config: &FailureReporting, time: i64) -> Result<Option<Report>> {
        let row = self.connection()?.query_row(
            "SELECT r.fingerprint,r.issue_number,r.action,r.pending_body,r.pending_sequence,r.uncertain,r.attempt,i.sequence,i.metadata,i.occurrences,i.first_at,i.last_at,i.state,(i.consecutive>=?2 AND i.active_since<=?3) FROM failure_reports r JOIN failure_incidents i ON i.fingerprint=r.fingerprint WHERE r.repository=?1 AND r.state!='idle' AND r.next_at<=?4 AND (r.action IS NOT NULL OR r.uncertain OR (i.sequence>r.notified_sequence AND (r.issue_number IS NOT NULL OR (i.consecutive>=?2 AND i.active_since<=?3)))) AND (r.action IS NOT NULL OR r.uncertain OR r.last_notified=0 OR r.last_notified<=?5) ORDER BY r.next_at,r.fingerprint LIMIT 1",
            params![config.repository,config.threshold,time.saturating_sub(config.debounce_seconds as i64),time,time.saturating_sub(config.update_seconds as i64)],
            |r| Ok(Report {fingerprint:r.get(0)?,issue:r.get(1)?,action:r.get(2)?,body:r.get(3)?,pending_sequence:r.get(4)?,uncertain:r.get(5)?,attempt:r.get(6)?,sequence:r.get(7)?,metadata:serde_json::from_str(&r.get::<_,String>(8)?).unwrap_or(Value::Null),occurrences:r.get(9)?,first:r.get(10)?,last:r.get(11)?,state:r.get(12)?,eligible:r.get(13)?}),
        ).optional()?;
        Ok(row)
    }
    fn report_body(&self, report: &Report, credential: &str) -> Result<String> {
        ensure!(report.metadata.is_object(), "invalid report metadata");
        let conn = self.connection()?;
        let windows: i64 = conn.query_row("SELECT count(DISTINCT window_key) FROM failure_events WHERE fingerprint=?1 AND outcome='failed' AND sequence<=?2",params![report.fingerprint,report.sequence],|r|r.get(0))?;
        let samples = conn.prepare("SELECT public_sample FROM failure_events WHERE fingerprint=?1 AND outcome='failed' AND sequence<=?2 ORDER BY sequence DESC LIMIT 6")?.query_map(params![report.fingerprint,report.sequence],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let m = &report.metadata;
        let mut body = format!(
            "<!-- da08-fingerprint:{} -->\nAutomatic persistent failure report (DA08 / #75).\n\nSubsystem: {}\nProvider: {}\nOperation: {}\nError class: {}\nApplication version: {}\nProvider schema: {}\nOccurrences: {}\nAffected windows: {}\nFirst observed (Unix UTC seconds): {}\nLast observed (Unix UTC seconds): {}\nCurrent state: {}\n\nExpected: the configured collection or publication operation completes and retains verified evidence.\nObserved: the operation failed with the diagnostics below.\n\nReproduction: use the same application/provider versions and operation on the listed public canonical source. For a private incident, an authorized operator must inspect its local record; no protected content is attached.\n\nLatest failed sources (at most six observations):\n",
            report.fingerprint,
            m["subsystem"],
            m["provider"],
            m["operation"],
            m["error_class"],
            m["application"],
            m["provider_schema"],
            report.occurrences,
            windows,
            report.first,
            report.last,
            report.state
        );
        for sample in samples {
            let s: Value = serde_json::from_str(&sample)?;
            let mut detail = String::new();
            if let Some(url) = s["url"].as_str() {
                detail.push_str(&format!("\nPublic source: <{url}>\n"));
            } else {
                detail.push_str(&format!(
                    "\nPrivate incident: {}\n",
                    s["incident"].as_str().unwrap_or("invalid")
                ));
            }
            detail.push_str(&format!(
                "```text\n{}\n```\n",
                s["error"]
                    .as_str()
                    .unwrap_or("Diagnostic unavailable; inspect the private incident.")
            ));
            if body.len() + detail.len() > 30000 {
                body.push_str("\nAdditional source excerpts omitted to bound this public report; aggregate counts include all retained observations.\n");
                break;
            }
            body.push_str(&detail);
        }
        // The dispatch credential is explicit authority, even when a caller forgot to classify it.
        let mut body = body.replace(credential, "[redacted]");
        let body_digest = digest(&body);
        body.push_str(&format!("\n<!-- da08-report:{body_digest} -->\n"));
        ensure!(body.len() <= 32768, "public report exceeds bound");
        Ok(body)
    }
    fn report_error(
        &self,
        config: &FailureReporting,
        report: &Report,
        time: i64,
        error: &str,
    ) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute("UPDATE failure_reports SET state=CASE WHEN action IS NOT NULL OR uncertain THEN 'reconcile' WHEN ?1='pending_configuration' THEN 'pending_configuration' ELSE 'retry_wait' END,error=?1,next_at=?2 WHERE repository=?3 AND fingerprint=?4",params![error,time.saturating_add(config.retry_seconds as i64),config.repository,report.fingerprint])?;
        tx.execute("UPDATE failure_report_attempts SET finished_at=?1,outcome=?2 WHERE repository=?3 AND fingerprint=?4 AND number=?5 AND finished_at IS NULL",params![time,error,config.repository,report.fingerprint,report.attempt])?;
        tx.commit()?;
        Ok(())
    }
    fn report_bind_issue(
        &self,
        config: &FailureReporting,
        report: &mut Report,
        issue: &Issue,
    ) -> Result<()> {
        self.connection()?.execute("UPDATE failure_reports SET issue_number=?1,issue_url=?2,uncertain=CASE WHEN action IS NULL THEN 0 ELSE uncertain END WHERE repository=?3 AND fingerprint=?4",params![issue.number,issue.url,config.repository,report.fingerprint])?;
        report.issue = Some(issue.number);
        Ok(())
    }
    fn report_begin(
        &self,
        config: &FailureReporting,
        report: &mut Report,
        action: &str,
        body: String,
        time: i64,
    ) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let n = report.attempt + 1;
        ensure!(tx.execute("UPDATE failure_reports SET action=?1,pending_body=?2,pending_sequence=?3,uncertain=1,attempt=?4,state='reconcile',error=NULL WHERE repository=?5 AND fingerprint=?6 AND action IS NULL",params![action,body,report.sequence,n,config.repository,report.fingerprint])?==1,"report dispatch ownership changed");
        tx.execute("INSERT INTO failure_report_attempts(repository,fingerprint,number,action,body_digest,started_at) VALUES(?1,?2,?3,?4,?5,?6)",params![config.repository,report.fingerprint,n,action,digest(&body),time])?;
        tx.commit()?;
        report.action = Some(action.to_owned());
        report.body = Some(body);
        report.pending_sequence = Some(report.sequence);
        report.attempt = n;
        report.uncertain = true;
        Ok(())
    }
    fn report_finish(&self, config: &FailureReporting, report: &Report, time: i64) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let delivered = report.action.as_deref() != Some("reopen");
        tx.execute("UPDATE failure_reports SET notified_sequence=CASE WHEN ?1 THEN ?2 ELSE notified_sequence END,last_notified=CASE WHEN ?1 THEN ?3 ELSE last_notified END,state=CASE WHEN ?1 AND (SELECT sequence FROM failure_incidents WHERE fingerprint=?5)<=?2 THEN 'idle' ELSE 'pending' END,action=NULL,pending_body=NULL,pending_sequence=NULL,uncertain=0,error=NULL,next_at=?3 WHERE repository=?4 AND fingerprint=?5",params![delivered,report.pending_sequence,time,config.repository,report.fingerprint])?;
        tx.execute("UPDATE failure_report_attempts SET finished_at=coalesce(finished_at,?1),outcome=coalesce(outcome,'verified'),verified_at=?1 WHERE repository=?2 AND fingerprint=?3 AND number=?4",params![time,config.repository,report.fingerprint,report.attempt])?;
        tx.commit()?;
        Ok(())
    }
}

#[derive(Deserialize)]
struct Comment {
    body: String,
}
#[derive(Deserialize)]
struct Issue {
    number: u64,
    url: String,
    state: String,
    body: String,
    #[serde(default)]
    comments: Vec<Comment>,
}
impl Issue {
    fn validate(&self, repository: &str, fingerprint: &str) -> bool {
        self.number > 0
            && self.url == format!("https://github.com/{repository}/issues/{}", self.number)
            && matches!(self.state.as_str(), "OPEN" | "CLOSED")
            && self
                .body
                .contains(&format!("<!-- da08-fingerprint:{fingerprint} -->"))
            && self.comments.len() <= 1000
    }
}
struct Gh<'a> {
    config: &'a FailureReporting,
    token: String,
    workspace: tempfile::TempDir,
    deadline: Instant,
}
impl Gh<'_> {
    async fn run(
        &self,
        args: Vec<String>,
        cancel: &CancellationToken,
    ) -> std::result::Result<String, &'static str> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() || cancel.is_cancelled() {
            return Err("interrupted");
        }
        let environment = BTreeMap::from([
            (OsString::from("GH_TOKEN"), OsString::from(&self.token)),
            ("GH_HOST".into(), "github.com".into()),
            ("GH_PROMPT_DISABLED".into(), "1".into()),
            ("GH_NO_UPDATE_NOTIFIER".into(), "1".into()),
            ("GH_NO_EXTENSION_UPDATE_NOTIFIER".into(), "1".into()),
            (
                "GH_CONFIG_DIR".into(),
                self.workspace.path().join("gh-config").into_os_string(),
            ),
        ]);
        let tool = Tool {
            executable: self.config.executable.clone().expect("readiness checked"),
            args: args.into_iter().map(OsString::from).collect(),
            environment,
            directory: self.workspace.path().to_owned(),
            timeout: remaining,
            log_bytes: 8192,
            stdout_bytes: Some(1048576),
            work_bytes: 2097152,
            secrets: vec![self.token.clone()],
        };
        match tool.run(cancel).await {
            Ok(out) => Ok(out.stdout),
            Err(e)
                if e.kind == ToolFailure::Missing
                    || e.report.as_ref().is_some_and(|r| r.exit_code == Some(4)) =>
            {
                Err("pending_configuration")
            }
            Err(_) => Err("gh_operation_failed"),
        }
    }
    fn args(&self, command: &str) -> Vec<String> {
        vec![
            "issue".into(),
            command.into(),
            "--repo".into(),
            self.config.repository.clone(),
        ]
    }
    async fn search(
        &self,
        fingerprint: &str,
        cancel: &CancellationToken,
    ) -> std::result::Result<Option<Issue>, &'static str> {
        let mut args = self.args("list");
        args.extend([
            "--state".into(),
            "all".into(),
            "--search".into(),
            format!("\"da08-fingerprint:{fingerprint}\""),
            "--limit".into(),
            "100".into(),
            "--json".into(),
            "number,url,state,body".into(),
        ]);
        let out = self.run(args, cancel).await?;
        let issues: Vec<Issue> = serde_json::from_str(&out).map_err(|_| "invalid_gh_response")?;
        if issues.len() >= 100 {
            return Err("ambiguous_issue_search");
        }
        let mut matches = issues.into_iter().filter(|i| {
            i.body
                .contains(&format!("<!-- da08-fingerprint:{fingerprint} -->"))
        });
        let found = matches.next();
        if matches.next().is_some() {
            return Err("ambiguous_issue_search");
        }
        if found
            .as_ref()
            .is_some_and(|i| !i.validate(&self.config.repository, fingerprint))
        {
            return Err("invalid_issue_identity");
        }
        Ok(found)
    }
    async fn view(
        &self,
        number: u64,
        fingerprint: &str,
        cancel: &CancellationToken,
    ) -> std::result::Result<Issue, &'static str> {
        let mut args = self.args("view");
        args.extend([
            number.to_string(),
            "--json".into(),
            "number,url,state,body,comments".into(),
        ]);
        let out = self.run(args, cancel).await?;
        let issue: Issue = serde_json::from_str(&out).map_err(|_| "invalid_gh_response")?;
        if issue.number != number || !issue.validate(&self.config.repository, fingerprint) {
            return Err("remote_issue_conflict");
        }
        Ok(issue)
    }
    async fn write(
        &self,
        report: &Report,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), &'static str> {
        let action = report.action.as_deref().ok_or("missing_intent")?;
        let mut args = self.args(action);
        if action == "create" {
            args.extend([
                "--title".into(),
                format!(
                    "[DA08] {} / {} / {}",
                    report.metadata["subsystem"].as_str().unwrap_or("unknown"),
                    report.metadata["operation"].as_str().unwrap_or("unknown"),
                    report.metadata["error_class"].as_str().unwrap_or("unknown")
                ),
            ]);
            for label in &self.config.labels {
                args.extend(["--label".into(), label.clone()]);
            }
        } else {
            args.push(report.issue.ok_or("missing_issue")?.to_string());
        }
        if action != "reopen" {
            let path = self.workspace.path().join("body.md");
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&path)
                .map_err(|_| "body_file_failed")?;
            file.write_all(report.body.as_deref().ok_or("missing_intent")?.as_bytes())
                .and_then(|_| file.sync_all())
                .map_err(|_| "body_file_failed")?;
            args.extend(["--body-file".into(), path.to_string_lossy().into_owned()]);
        }
        self.run(args, cancel).await?;
        Ok(())
    }
}

/// A single bounded automatic pass; `time` is Unix UTC seconds (runtime uses the system clock).
pub async fn tick(
    store: &Store,
    config: &FailureReporting,
    time: i64,
    cancel: &CancellationToken,
) -> Result<bool> {
    config.validate()?;
    let Some(_guard) = store.report_lock()? else {
        return Ok(false);
    };
    store.bind_reports(config)?;
    if !config.enabled || cancel.is_cancelled() || store.recovery_paused()? {
        return Ok(false);
    }
    let Some(mut report) = store.report_next(config, time)? else {
        return Ok(false);
    };
    let token = match failure_reporting::credentials(config) {
        Ok(t) => t,
        Err(_) => {
            store.report_error(config, &report, time, "pending_configuration")?;
            return Ok(false);
        }
    };
    let gh = Gh {
        config,
        token,
        workspace: store.workspace(&format!("report-{}", &report.fingerprint[..32]))?,
        deadline: Instant::now() + Duration::from_secs(config.timeout_seconds),
    };
    let result = dispatch(store, &gh, &mut report, time, cancel).await;
    if let Err(error) = result {
        store.report_error(config, &report, time, error)?;
    }
    Ok(true)
}
async fn dispatch(
    store: &Store,
    gh: &Gh<'_>,
    r: &mut Report,
    time: i64,
    cancel: &CancellationToken,
) -> std::result::Result<(), &'static str> {
    let mut issue = if let Some(number) = r.issue {
        Some(gh.view(number, &r.fingerprint, cancel).await?)
    } else {
        gh.search(&r.fingerprint, cancel).await?
    };
    if let Some(found) = &issue {
        store
            .report_bind_issue(gh.config, r, found)
            .map_err(|_| "local_state")?;
    }
    if let Some(action) = r.action.as_deref() {
        if action == "create" && issue.is_some() {
            issue = Some(
                gh.view(r.issue.ok_or("missing_issue")?, &r.fingerprint, cancel)
                    .await?,
            );
        }
        let expected = r.body.as_deref().ok_or("missing_intent")?;
        let verified = issue.as_ref().is_some_and(|i| match action {
            "create" => i.body == expected,
            "comment" => i.comments.iter().any(|c| c.body == expected),
            "reopen" => i.state == "OPEN",
            _ => false,
        });
        if verified {
            return store
                .report_finish(gh.config, r, time)
                .map_err(|_| "local_state");
        }
        // Reopen is idempotent. A create/comment with an unknown outcome is never blindly repeated.
        if action != "reopen" {
            return Err("unconfirmed_remote_write");
        }
    } else {
        if issue.is_none() && r.uncertain {
            return Err("unconfirmed_remote_write");
        }
        let body = store.report_body(r, &gh.token).map_err(|_| "local_state")?;
        let action = if issue.as_ref().is_some_and(|i| i.state == "CLOSED")
            && r.state == "active"
            && r.eligible
        {
            "reopen"
        } else if issue.is_some() {
            "comment"
        } else {
            "create"
        };
        store
            .report_begin(gh.config, r, action, body, time)
            .map_err(|_| "local_state")?;
    }
    gh.write(r, cancel).await?;
    if r.issue.is_none() {
        // Create stdout is not evidence. Search, bind identity, and read the returned body.
        let found = gh
            .search(&r.fingerprint, cancel)
            .await?
            .ok_or("unconfirmed_remote_write")?;
        store
            .report_bind_issue(gh.config, r, &found)
            .map_err(|_| "local_state")?;
    }
    let observed = gh
        .view(r.issue.ok_or("missing_issue")?, &r.fingerprint, cancel)
        .await?;
    let expected = r.body.as_deref().ok_or("missing_intent")?;
    let verified = match r.action.as_deref() {
        Some("create") => observed.body == expected,
        Some("comment") => observed.comments.iter().any(|c| c.body == expected),
        Some("reopen") => observed.state == "OPEN",
        _ => false,
    };
    if !verified {
        return Err("unconfirmed_remote_write");
    }
    store
        .report_finish(gh.config, r, time)
        .map_err(|_| "local_state")
}
