//! DA08 / #75: private durable accounting and an explicit public diagnostic boundary.
//! Callers must classify expected gaps and must opt in to disclosing a canonical public URL.
use crate::{config::FailureReporting, digest, storage::Store};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::os::unix::fs::PermissionsExt;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Subsystem {
    Timeline,
    Capture,
    Notion,
    Reporter,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    Transport,
    InvalidResponse,
    Extraction,
    Integrity,
    Internal,
    Deleted,
    Private,
    Unavailable,
    RateLimited,
    PendingConfiguration,
    RemoteConflict,
    Cancelled,
}
impl ErrorClass {
    fn actionable(self) -> bool {
        matches!(
            self,
            Self::Transport
                | Self::InvalidResponse
                | Self::Extraction
                | Self::Integrity
                | Self::Internal
        )
    }

    pub(crate) fn from_job(error: crate::jobs::Failure) -> Self {
        use crate::jobs::Failure;
        match error {
            Failure::Network | Failure::Timeout | Failure::Unavailable => Self::Transport,
            Failure::RateLimited => Self::RateLimited,
            Failure::InvalidInput | Failure::ToolMissing => Self::PendingConfiguration,
            Failure::InvalidOutput => Self::InvalidResponse,
            Failure::Cancelled | Failure::LeaseLost => Self::Cancelled,
            Failure::ToolExit | Failure::ModelTruncated | Failure::ModelRefused => Self::Extraction,
            Failure::DiskFull | Failure::Internal => Self::Internal,
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Failed,
    Recovered,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "visibility", content = "reference", rename_all = "snake_case")]
pub enum Source {
    /// Only a canonical, intentionally public source, never a signed media download URL.
    PublicUrl(String),
    /// The reference stays in the private database; only its digest is public.
    Private(String),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailureEvent {
    /// Stable caller-owned observation/attempt ID; replaying the same event is idempotent.
    pub event_id: String,
    /// Stable operation/source stream across attempts; recovery clears only this stream.
    pub stream_id: String,
    pub subsystem: Subsystem,
    /// Protocol tags, not account names, URLs, tokens, or free-form exceptions.
    pub provider: String,
    pub operation: String,
    pub error_class: ErrorClass,
    pub provider_schema: String,
    pub observed_at: i64,
    pub outcome: Outcome,
    pub window_id: Option<String>,
    pub source: Source,
    /// Explicitly publishable excerpt. Never pass an unfiltered HTTP response or protected body.
    pub public_excerpt: Option<String>,
    /// Exact diagnostics remain only in the private full backup, never semantic Git export.
    pub private_details: String,
    /// Known secrets/private strings to remove; not retained in any serialized record.
    #[serde(skip)]
    pub redactions: Vec<String>,
}

fn tag(value: &str) -> Result<String> {
    let value = value.trim().to_ascii_lowercase();
    ensure!(
        !value.is_empty()
            && value.len() <= 64
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b)),
        "failure protocol tag is invalid"
    );
    Ok(value)
}
impl FailureEvent {
    fn metadata(&self) -> Result<Value> {
        Ok(
            json!({"version":1,"subsystem":self.subsystem,"provider":tag(&self.provider)?,"operation":tag(&self.operation)?,"error_class":self.error_class,"application":env!("CARGO_PKG_VERSION"),"provider_schema":tag(&self.provider_schema)?}),
        )
    }
    pub fn fingerprint(&self) -> Result<String> {
        Ok(digest(serde_json::to_vec(&self.metadata()?)?))
    }
}

/// A bounded, private producer boundary inside the caller's settlement transaction.
/// `None` means verified success, and only clears matching, currently active classes.
pub(crate) struct Producer<'a> {
    pub subsystem: Subsystem,
    pub provider: &'a str,
    pub operation: &'a str,
    pub schema: &'a str,
    pub stream: &'a str,
    pub source: &'a str,
    pub window: Option<&'a str>,
}
impl Producer<'_> {
    pub fn settle(
        &self,
        tx: &Transaction<'_>,
        attempt: &str,
        time: i64,
        failure: Option<ErrorClass>,
    ) -> Result<()> {
        let stream_key = digest(self.stream);
        if failure.is_none()
            && !tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM failure_streams WHERE stream_key=?1 AND consecutive>0)",
                [&stream_key],
                |r| r.get::<_, bool>(0),
            )?
        {
            return Ok(());
        }
        let classes = [
            ErrorClass::Transport,
            ErrorClass::InvalidResponse,
            ErrorClass::Extraction,
            ErrorClass::Integrity,
            ErrorClass::Internal,
            ErrorClass::Deleted,
            ErrorClass::Private,
            ErrorClass::Unavailable,
            ErrorClass::RateLimited,
            ErrorClass::PendingConfiguration,
            ErrorClass::RemoteConflict,
            ErrorClass::Cancelled,
        ];
        for class in classes {
            if failure.is_some_and(|failed| failed != class) {
                continue;
            }
            let outcome = if failure.is_some() {
                Outcome::Failed
            } else {
                Outcome::Recovered
            };
            let event_id = format!("{}:{attempt}:{class:?}:{outcome:?}", self.stream);
            let observed_at = tx
                .query_row(
                    "SELECT observed_at FROM failure_events WHERE event_id=?1",
                    [digest(&event_id)],
                    |r| r.get::<_, i64>(0),
                )
                .optional()?
                .unwrap_or(time);
            let event = FailureEvent {
                event_id,
                stream_id: self.stream.into(),
                subsystem: self.subsystem,
                provider: self.provider.into(),
                operation: self.operation.into(),
                error_class: class,
                provider_schema: self.schema.into(),
                observed_at,
                outcome,
                window_id: self.window.map(str::to_owned),
                source: Source::Private(self.source.into()),
                public_excerpt: None,
                private_details: format!("{} settlement: {class:?}", self.operation),
                redactions: Vec::new(),
            };
            if failure.is_none() && !tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM failure_streams WHERE stream_key=?1 AND fingerprint=?2 AND consecutive>0)",
                params![stream_key,event.fingerprint()?], |r| r.get::<_, bool>(0),
            )? {
                continue;
            }
            record_in_tx(tx, &event)?;
        }
        Ok(())
    }
}

pub(crate) fn timeline_settled(
    tx: &Transaction<'_>,
    lease: &crate::planner::DailyLease,
    evidence: &Value,
    reason: Option<&str>,
    complete: bool,
    attempt: &str,
    time: i64,
) -> Result<()> {
    let Some(provider @ ("fx-twitter-v2" | "x-web-session-v1")) = evidence["provider"].as_str()
    else {
        return Ok(());
    };
    let class = if complete {
        None
    } else {
        Some(match reason {
            Some(
                "account_lookup_transport"
                | "timeline_transport"
                | "provider_http_failure"
                | "provider_api_failure"
                | "provider_graphql_error"
                | "provider_unavailable",
            ) => ErrorClass::Transport,
            Some("provider_invalid_json" | "invalid_timeline_page") => ErrorClass::InvalidResponse,
            Some("collection_state_or_storage_error") => ErrorClass::Internal,
            Some(
                "authentication_or_access_denied"
                | "authentication_or_challenge"
                | "timeline_provider_changed"
                | "provider_pending_configuration",
            ) => ErrorClass::PendingConfiguration,
            Some("rate_limited") => ErrorClass::RateLimited,
            Some("cancelled") => ErrorClass::Cancelled,
            Some(
                "coverage_gaps"
                | "cursor_or_coverage_gap"
                | "cursor_stalled"
                | "provider_scan_budget"
                | "account_identity_unavailable"
                | "timeline_capability_unavailable"
                | "recovered_terminal_page",
            ) => ErrorClass::Unavailable,
            _ => return Ok(()),
        })
    };
    Producer {
        subsystem: Subsystem::Timeline,
        provider,
        operation: "collection",
        schema: "timeline-v1",
        stream: &format!("timeline:{}", lease.window.plan.id),
        source: &lease.window.plan.id,
        window: Some(&lease.window.plan.id),
    }
    .settle(tx, attempt, time, class)
}

/// The caller declares the canonical source public; strip authentication and ephemeral parameters.
fn public_url(input: &str, redactions: &[String]) -> Option<String> {
    if input.len() > 4096 {
        return None;
    }
    let mut u = url::Url::parse(input).ok()?;
    u.set_username("").ok()?;
    u.set_password(None).ok()?;
    let pairs: Vec<(String, String)> = u
        .query_pairs()
        .filter(|(key, value)| {
            let key = key.to_ascii_lowercase();
            ![
                "token",
                "secret",
                "key",
                "auth",
                "cookie",
                "signature",
                "credential",
                "expire",
                "signed",
                "x-amz",
                "x-goog",
                "session",
                "password",
            ]
            .iter()
            .any(|s| key.contains(s))
                && !key.starts_with("utm_")
                && key != "fbclid"
                && key != "gclid"
                && value.len() <= 512
                && !redactions
                    .iter()
                    .any(|s| !s.is_empty() && value.contains(s))
        })
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    u.set_query(None);
    u.set_fragment(None);
    if !pairs.is_empty() {
        u.query_pairs_mut().extend_pairs(pairs);
    }
    let host = u.host_str()?;
    if host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || !host.contains('.')
    {
        return None;
    }
    crate::network::source_url(u.as_str()).ok()?;
    let value = u.to_string();
    if redactions
        .iter()
        .any(|s| !s.is_empty() && value.contains(s))
    {
        return None;
    }
    Some(value)
}

/// Notion publishes private canonical records; never treat those original URLs as public by default.
pub(crate) fn notion_settled(
    tx: &Transaction<'_>,
    id: &str,
    attempt: i64,
    result: &std::result::Result<
        Option<crate::notion::VerifiedPage>,
        crate::notion::PublicationError,
    >,
) -> Result<()> {
    use crate::notion::Failure;
    let time = crate::storage::now();
    let stream = format!("notion:{id}");
    let class = match result {
        Ok(Some(_)) => None,
        Ok(None) => Some(ErrorClass::Unavailable),
        Err(e) => Some(match e.state {
            Failure::Disabled
            | Failure::CompiledOut
            | Failure::PendingConfiguration
            | Failure::UnsupportedTool
            | Failure::BlockedAuth => ErrorClass::PendingConfiguration,
            Failure::RetryWait => ErrorClass::RateLimited,
            Failure::Conflict => ErrorClass::RemoteConflict,
            Failure::Cancelled => ErrorClass::Cancelled,
            Failure::InvalidPayload | Failure::Limit => ErrorClass::Internal,
            Failure::InvalidOutput => ErrorClass::InvalidResponse,
            Failure::Reconcile | Failure::Unavailable => ErrorClass::Transport,
        }),
    };
    let classes = if let Some(class) = class {
        vec![class]
    } else {
        let metadata=tx.prepare("SELECT i.metadata FROM failure_streams s JOIN failure_incidents i ON i.fingerprint=s.fingerprint WHERE s.stream_key=?1 AND s.consecutive>0")?.query_map([digest(&stream)],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        metadata
            .into_iter()
            .map(|m| {
                Ok(serde_json::from_value::<ErrorClass>(
                    serde_json::from_str::<Value>(&m)?["error_class"].clone(),
                )?)
            })
            .collect::<Result<Vec<_>>>()?
    };
    for c in classes {
        let event_id = format!(
            "{stream}:{attempt}:{c:?}:{}",
            if class.is_some() {
                "failed"
            } else {
                "recovered"
            }
        );
        // An operator may revalidate the same immutable record before any dispatch. Its attempt
        // identity has not advanced; preserve the first observation instead of colliding on time.
        let observed_at = tx
            .query_row(
                "SELECT observed_at FROM failure_events WHERE event_id=?1",
                [digest(&event_id)],
                |r| r.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(time);
        let event = FailureEvent {
            event_id,
            stream_id: stream.clone(),
            subsystem: Subsystem::Notion,
            provider: "notion".into(),
            operation: "publication".into(),
            error_class: c,
            provider_schema: "2026-03-11".into(),
            observed_at,
            outcome: if class.is_some() {
                Outcome::Failed
            } else {
                Outcome::Recovered
            },
            window_id: None,
            source: Source::Private(id.to_owned()),
            public_excerpt: None,
            private_details: format!("Notion publication settlement: {c:?}"),
            redactions: Vec::new(),
        };
        record_in_tx(tx, &event)?;
    }
    Ok(())
}
fn excerpt(input: &str, redactions: &[String]) -> String {
    let mut value = input.to_owned();
    for secret in redactions.iter().filter(|s| !s.is_empty()) {
        value = value.replace(secret, "[redacted]");
    }
    let mut output = String::new();
    for line in value.lines().take(24) {
        let lower = line.to_ascii_lowercase();
        if [
            "authorization",
            "cookie",
            "password",
            "secret",
            "api_key",
            "api-key",
            "access_token",
            "refresh_token",
            "signature",
            "credential",
            "private_body",
            "private_account",
        ]
        .iter()
        .any(|k| lower.contains(k))
        {
            output.push_str("[sensitive diagnostic line removed]\n");
        } else {
            for word in line.split_whitespace() {
                let lower = word.to_ascii_lowercase();
                let safe = if lower.contains("://")
                    || lower.starts_with("ghp_")
                    || lower.starts_with("github_pat_")
                    || lower.starts_with("sk-")
                {
                    "[redacted]"
                } else {
                    word
                };
                for c in safe.chars().filter(|c| !c.is_control()).take(256) {
                    // Avoid Markdown fences, HTML markers, or notification mentions in excerpts.
                    output.push(match c {
                        '`' => '\'',
                        '<' => '[',
                        '>' => ']',
                        '@' => '＠',
                        _ => c,
                    });
                }
                output.push(' ');
                if output.len() >= 2048 {
                    break;
                }
            }
            output.push('\n');
        }
        if output.len() >= 2048 {
            break;
        }
    }
    output
        .chars()
        .take(2048)
        .collect::<String>()
        .trim()
        .to_owned()
}
fn sample(event: &FailureEvent) -> Value {
    let incident = digest(serde_json::to_vec(&event.source).expect("source is serializable"));
    let url = match &event.source {
        Source::PublicUrl(u) => public_url(u, &event.redactions),
        Source::Private(_) => None,
    };
    let message = if url.is_some() {
        event
            .public_excerpt
            .as_deref()
            .map(|s| excerpt(s, &event.redactions))
            .filter(|s| !s.is_empty())
    } else {
        None
    };
    json!({"incident":incident,"url":url,"error":message.unwrap_or_else(|| format!("{:?} failure; exact diagnostics retained in the private incident record.",event.error_class))})
}

/// Shared transaction seam for collectors and publication settlement. Never invokes a tool.
pub(crate) fn record_in_tx(tx: &Transaction<'_>, event: &FailureEvent) -> Result<Option<String>> {
    if event.subsystem == Subsystem::Reporter {
        return Ok(None);
    }
    ensure!(
        !event.event_id.is_empty()
            && event.event_id.len() <= 512
            && !event.stream_id.is_empty()
            && event.stream_id.len() <= 512
            && event.observed_at >= 0
            && event.private_details.len() <= 65536
            && event
                .public_excerpt
                .as_ref()
                .is_none_or(|s| s.len() <= 16384)
            && event.window_id.as_ref().is_none_or(|s| s.len() <= 512)
            && event.redactions.len() <= 64
            && event.redactions.iter().all(|s| s.len() <= 16384),
        "failure event exceeds bounds"
    );
    let encoded = serde_json::to_string(event)?;
    ensure!(encoded.len() <= 98304, "failure event exceeds record bound");
    let id = digest(&event.event_id);
    let fingerprint = event.fingerprint()?;
    if let Some((previous, stored_fingerprint)) = tx
        .query_row(
            "SELECT private_record,fingerprint FROM failure_events WHERE event_id=?1",
            [&id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()?
    {
        ensure!(previous == encoded, "failure event identity collision");
        return Ok(Some(stored_fingerprint));
    }
    let metadata = serde_json::to_string(&event.metadata()?)?;
    let failed = event.outcome == Outcome::Failed;
    tx.execute("INSERT OR IGNORE INTO failure_incidents(fingerprint,metadata,first_at,last_at,actionable,state) VALUES(?1,?2,?3,?3,?4,'recovered')", params![fingerprint,metadata,event.observed_at,event.error_class.actionable()])?;
    tx.execute("INSERT INTO failure_events(event_id,fingerprint,observed_at,sequence,outcome,window_key,private_record,public_sample) SELECT ?1,?2,?3,sequence+1,?4,?5,?6,?7 FROM failure_incidents WHERE fingerprint=?2",params![id,fingerprint,event.observed_at,if failed {"failed"} else {"recovered"},event.window_id.as_ref().map(digest),encoded,serde_json::to_string(&sample(event))?])?;
    let stream = digest(&event.stream_id);
    let prior: i64 = tx
        .query_row(
            "SELECT consecutive FROM failure_streams WHERE fingerprint=?1 AND stream_key=?2",
            params![fingerprint, stream],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);
    let count = if failed { prior.saturating_add(1) } else { 0 };
    tx.execute("INSERT INTO failure_streams(fingerprint,stream_key,consecutive,active_since) VALUES(?1,?2,?3,?4) ON CONFLICT(fingerprint,stream_key) DO UPDATE SET consecutive=excluded.consecutive,active_since=CASE WHEN excluded.consecutive=0 THEN NULL ELSE coalesce(failure_streams.active_since,excluded.active_since) END",params![fingerprint,stream,count,if failed {Some(event.observed_at)} else {None}])?;
    // One recovered source cannot clear another unresolved source in the same defect class.
    tx.execute("UPDATE failure_incidents SET occurrences=occurrences+?1,consecutive=consecutive+?2,first_at=min(first_at,?3),last_at=max(last_at,?3),active_since=(SELECT min(active_since) FROM failure_streams WHERE fingerprint=?4 AND consecutive>0),sequence=sequence+1,state=CASE WHEN consecutive+?2>0 THEN 'active' ELSE 'recovered' END WHERE fingerprint=?4",params![failed,count-prior,event.observed_at,fingerprint])?;
    Ok(Some(fingerprint))
}
impl Store {
    pub fn record_failure(&self, event: &FailureEvent) -> Result<Option<String>> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let result = record_in_tx(&tx, event)?;
        tx.commit()?;
        Ok(result)
    }
    /// Content-free, bounded status. Private source URLs and diagnostic text are never returned.
    pub fn failure_report_status(&self, limit: usize) -> Result<Value> {
        ensure!(
            (1..=100).contains(&limit),
            "failure status limit must be 1..100"
        );
        let conn = self.connection()?;
        let incidents: i64 =
            conn.query_row("SELECT count(*) FROM failure_incidents", [], |r| r.get(0))?;
        let events: i64 =
            conn.query_row("SELECT count(*) FROM failure_events", [], |r| r.get(0))?;
        let rows = conn.prepare("SELECT repository,fingerprint,state,issue_number,error FROM failure_reports ORDER BY repository,fingerprint LIMIT ?1")?.query_map([limit],|r|Ok(json!({"repository":r.get::<_,String>(0)?,"fingerprint":r.get::<_,String>(1)?,"state":r.get::<_,String>(2)?,"issue_number":r.get::<_,Option<i64>>(3)?,"error":r.get::<_,Option<String>>(4)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(json!({"events":events,"incidents":incidents,"reports":rows}))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    Disabled,
    PendingConfiguration,
    Ready,
}
pub fn readiness(config: &FailureReporting) -> Readiness {
    if !config.enabled {
        return Readiness::Disabled;
    }
    if credentials(config).is_ok() {
        Readiness::Ready
    } else {
        Readiness::PendingConfiguration
    }
}
pub(crate) fn credentials(config: &FailureReporting) -> Result<String> {
    ensure!(config.enabled, "reporting disabled");
    config.validate()?;
    let path = config
        .executable
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("gh not configured"))?;
    let meta = std::fs::metadata(path)?;
    ensure!(
        meta.is_file() && meta.permissions().mode() & 0o111 != 0,
        "gh unavailable"
    );
    let token = config
        .token
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("gh token not configured"))?
        .read()?;
    ensure!(
        token.len() <= 16384 && !token.chars().any(char::is_whitespace),
        "invalid gh token"
    );
    Ok(token)
}
