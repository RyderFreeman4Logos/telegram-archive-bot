use super::{FxTimeline, Provider, Scan, WebTimeline, parse_page};
use crate::{
    activities::{AccountIdentity, ActivityCapture, DiscoveryRun},
    config::{Config, DailyProviderKind},
    planner::{DailyLease, DailyProgress, DayState},
    sources::{Capture, FetchStatus, Identity, ViewPolicy},
    storage::{Store, now},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u32,
    provider: String,
    scan: Scan,
    last_raw: Option<String>,
    overlap_raw: Option<String>,
}

pub async fn step(store: &Store, config: &Config, cancel: &CancellationToken) -> Result<bool> {
    if !config.daily.enabled
        || config.daily.provider.kind == DailyProviderKind::Disabled
        || cancel.is_cancelled()
        || store.recovery_paused()?
    {
        return Ok(false);
    }
    if config.daily.provider.kind == DailyProviderKind::FxTwitter {
        let provider = FxTimeline {
            http: crate::network::PublicHttp::default(),
            page_size: config.daily.provider.page_size,
            max_bytes: config.limits.input_bytes,
            timeout: std::time::Duration::from_secs((config.limits.lease_seconds / 3).clamp(1, 30)),
            #[cfg(test)]
            base: "https://api.fxtwitter.com",
        };
        return tick(store, config, &provider, cancel).await;
    }
    // Missing secrets are readiness, not a failed collection attempt.
    let Some(cookie) = config
        .daily
        .provider
        .cookie
        .as_ref()
        .and_then(|r| r.read().ok())
    else {
        return Ok(false);
    };
    let Some(bearer) = config
        .daily
        .provider
        .bearer
        .as_ref()
        .and_then(|r| r.read().ok())
    else {
        return Ok(false);
    };
    let provider = WebTimeline {
        http: crate::network::PublicHttp::default(),
        cookie,
        bearer,
        page_size: config.daily.provider.page_size,
        max_bytes: config.limits.input_bytes,
        timeout: std::time::Duration::from_secs((config.limits.lease_seconds / 3).clamp(1, 30)),
        #[cfg(test)]
        base: "https://x.com",
    };
    tick(store, config, &provider, cancel).await
}
pub async fn tick(
    store: &Store,
    config: &Config,
    provider: &dyn Provider,
    cancel: &CancellationToken,
) -> Result<bool> {
    if cancel.is_cancelled() {
        return Ok(false);
    }
    let Some(lease) = store.claim_daily_window(config, now())? else {
        return Ok(false);
    };
    if collect_lease(store, &lease, provider, cancel)
        .await
        .is_err()
    {
        store.finish_daily_window(
            &lease,
            DailyProgress {
                state: DayState::Blocked,
                cursor: store.daily_window(&lease.window.plan.id, now())?.cursor,
                reason: Some("collection_state_or_storage_error".into()),
                evidence: json!({"provider":provider.name()}),
                coverage_complete: false,
            },
            now(),
        )?;
    }
    Ok(true)
}
fn finish(
    store: &Store,
    lease: &DailyLease,
    cursor: &Cursor,
    state: DayState,
    reason: &str,
    evidence: Value,
    retry: Option<u64>,
) -> Result<()> {
    let complete = cursor.scan.complete();
    store.finish_daily_window_after(lease,DailyProgress{state,cursor:if cursor.scan.ended {None}else{Some(serde_json::to_string(cursor)?)},reason:Some(reason.into()),evidence:json!({"provider":cursor.provider,"scan":cursor.scan,"last_raw":cursor.last_raw,"outcome":evidence}),coverage_complete:complete},now(),retry)
}
fn classify(response: &crate::network::Response) -> Option<&'static str> {
    match response.status {
        401 | 403 => return Some("authentication_or_access_denied"),
        429 => return Some("rate_limited"),
        404 => return Some("timeline_capability_unavailable"),
        200 => {}
        _ => return Some("provider_http_failure"),
    }
    let value: Value = match serde_json::from_slice(&response.bytes) {
        Ok(value) => value,
        Err(_) => return Some("provider_invalid_json"),
    };
    if value.get("code").is_some_and(|code| code != 200) {
        return Some("provider_api_failure");
    }
    if let Some(errors) = value
        .get("errors")
        .and_then(Value::as_array)
        .filter(|e| !e.is_empty())
    {
        return Some(if errors.iter().any(|e| e["code"] == 88) {
            "rate_limited"
        } else if errors
            .iter()
            .any(|e| [32, 89, 326].iter().any(|c| e["code"] == *c))
        {
            "authentication_or_challenge"
        } else {
            "provider_graphql_error"
        });
    }
    None
}

pub async fn collect_lease(
    store: &Store,
    lease: &DailyLease,
    provider: &dyn Provider,
    cancel: &CancellationToken,
) -> Result<()> {
    let mut cursor = match &lease.window.cursor {
        Some(raw) => {
            let parsed: Cursor = serde_json::from_str(raw)?;
            ensure!(parsed.version == 1, "unsupported timeline cursor");
            parsed
        }
        None => Cursor {
            version: 1,
            provider: provider.name().into(),
            ..Cursor::default()
        },
    };
    if cursor.provider != provider.name() {
        return finish(
            store,
            lease,
            &cursor,
            DayState::Blocked,
            "timeline_provider_changed",
            json!({"requested_provider":provider.name()}),
            None,
        );
    }
    if let Some(raw) = &cursor.last_raw {
        store.verify_blob(raw)?;
    }
    if cursor.scan.ended {
        return finish(
            store,
            lease,
            &cursor,
            if cursor.scan.complete() {
                DayState::Complete
            } else {
                DayState::Blocked
            },
            "recovered_terminal_page",
            json!({}),
            None,
        );
    }
    let known = lease
        .resolved_account
        .as_ref()
        .unwrap_or(&lease.window.plan.account);
    let account = match known {
        AccountIdentity::ProviderId { user_id, .. } => user_id.clone(),
        AccountIdentity::Unresolved { .. } => {
            let handle = lease
                .watch
                .handle
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("missing account handle"))?;
            let response = match provider.lookup(handle, cancel).await {
                Ok(value) => value,
                Err(_) => {
                    return finish(
                        store,
                        lease,
                        &cursor,
                        DayState::Blocked,
                        "account_lookup_transport",
                        json!({}),
                        None,
                    );
                }
            };
            let raw = store.publish_bytes(&response.bytes)?;
            if let Some(reason) = classify(&response) {
                return finish(
                    store,
                    lease,
                    &cursor,
                    DayState::Blocked,
                    reason,
                    json!({"lookup_raw":raw.digest,"http_status":response.status}),
                    response.retry_after_seconds,
                );
            }
            let value: Value = serde_json::from_slice(&response.bytes)?;
            let Some(id) = value
                .pointer("/data/user/result/rest_id")
                .or_else(|| value.pointer("/user/id"))
                .and_then(Value::as_str)
            else {
                return finish(
                    store,
                    lease,
                    &cursor,
                    DayState::Blocked,
                    "account_identity_unavailable",
                    json!({"lookup_raw":raw.digest}),
                    None,
                );
            };
            Identity::x(id)?;
            store.bind_daily_account(
                lease,
                id,
                &json!({"raw_digest":raw.digest,"provider":provider.name(),"handle":handle}),
                now(),
            )?;
            id.to_owned()
        }
    };
    let actor = AccountIdentity::ProviderId {
        provider: "x".into(),
        user_id: account.clone(),
    };
    let mut request_cursor = if cursor.last_raw.is_some() {
        cursor.scan.overlap_cursor.clone()
    } else {
        cursor.scan.next_cursor.clone()
    };
    let mut overlap = cursor.last_raw.is_some();
    let initial_pages = cursor.scan.pages;
    for _ in 0..lease
        .config
        .daily
        .provider
        .max_pages
        .saturating_add(usize::from(overlap))
    {
        if cursor.scan.pages >= lease.config.daily.provider.max_scan_pages {
            return finish(
                store,
                lease,
                &cursor,
                DayState::Blocked,
                "provider_scan_budget",
                json!({"max_scan_pages":lease.config.daily.provider.max_scan_pages}),
                None,
            );
        }
        if cancel.is_cancelled() {
            return finish(
                store,
                lease,
                &cursor,
                DayState::Partial,
                "cancelled",
                json!({}),
                None,
            );
        }
        let response = match provider
            .page(&account, request_cursor.as_deref(), cancel)
            .await
        {
            Ok(value) => value,
            Err(_) => {
                return finish(
                    store,
                    lease,
                    &cursor,
                    DayState::Blocked,
                    "timeline_transport",
                    json!({}),
                    None,
                );
            }
        };
        let raw = store.publish_bytes(&response.bytes)?;
        let evidence = json!({"raw_digest":raw.digest,"raw_bytes":raw.size,"http_status":response.status,"request_cursor":request_cursor,"retry_after_seconds":response.retry_after_seconds});
        if let Some(reason) = classify(&response) {
            return finish(
                store,
                lease,
                &cursor,
                DayState::Blocked,
                reason,
                evidence,
                response.retry_after_seconds,
            );
        }
        let page = match parse_page(&account, &response.bytes, 10000) {
            Ok(value) => value,
            Err(_) => {
                return finish(
                    store,
                    lease,
                    &cursor,
                    DayState::Blocked,
                    "invalid_timeline_page",
                    evidence,
                    None,
                );
            }
        };
        let replayed_overlap = overlap;
        if overlap {
            overlap = false;
            let previous = store.read_blob(
                cursor
                    .last_raw
                    .as_deref()
                    .expect("overlap has raw evidence"),
                lease.config.limits.input_bytes,
            )?;
            let previous = parse_page(&account, &previous, 10000)
                .map_err(|_| anyhow::anyhow!("invalid retained timeline page"))?;
            cursor.overlap_raw = Some(raw.digest.clone());
            if serde_json::to_value(&previous)? == serde_json::to_value(&page)? {
                cursor.last_raw = Some(raw.digest);
                request_cursor = cursor.scan.next_cursor.clone();
                continue;
            }
            if let Some(next) = &cursor.scan.next_cursor {
                cursor.scan.seen_cursors.remove(&crate::digest(next));
            }
            cursor.scan.next_cursor = request_cursor.clone();
            cursor.scan.pages = cursor.scan.pages.saturating_sub(1);
            cursor.scan.gaps.insert("overlap_page_changed".into());
        }
        let selected = match cursor.scan.observe(
            &page,
            lease.window.plan.utc_start,
            lease.window.plan.utc_end,
        ) {
            Ok(value) => value,
            Err(_) => {
                return finish(
                    store,
                    lease,
                    &cursor,
                    DayState::Blocked,
                    "cursor_or_coverage_gap",
                    evidence,
                    None,
                );
            }
        };
        let retrieved_at = now();
        let verified_raw = store.verified_blob(&raw.digest)?;
        for index in selected {
            let item = &page.activities[index];
            let identity = Identity::x(&item.source_id)?;
            let capture = Capture {
                identity: identity.clone(),
                submitted_url: Some(identity.url()),
                content: None,
                status: FetchStatus::Missing,
                extractor: "x-timeline-v1".into(),
                evidence: json!({"reason":"awaiting_body_hydration","timeline_raw":raw.digest,"entry_id":item.entry_id}),
                raw_digest: Some(raw.digest.clone()),
                relations: item.relations.clone(),
            };
            let observation_key = crate::digest(serde_json::to_vec(&json!([
                lease.window.attempts,
                raw.digest,
                item.entry_id,
                retrieved_at,
                request_cursor
            ]))?);
            store.record_activity_verified(&ActivityCapture{actor:actor.clone(),handle:lease.watch.handle.clone(),display_name:lease.watch.label.clone(),provider_event_id:item.provider_event_id.clone(),kind:item.kind,discovery:DiscoveryRun{provider:"x".into(),key:lease.window.plan.id.clone(),provenance:json!({"window_id":lease.window.plan.id,"timezone":lease.window.plan.timezone,"utc_start":lease.window.plan.utc_start,"utc_end":lease.window.plan.utc_end})},occurrence_key:crate::digest(serde_json::to_vec(&json!([provider.name(),request_cursor,item.entry_id]))?),observation_key,activity_time:item.activity_time.clone(),source_time:item.source_time.clone(),retrieved_at,provenance:json!({"raw_digest":raw.digest,"entry_id":item.entry_id,"pinned":item.pinned,"provider":provider.name(),"provider_provenance":item.provenance,"window_id":lease.window.plan.id}),capture,view_policy:ViewPolicy{max_nodes:lease.config.limits.graph_nodes,max_depth:lease.config.limits.graph_depth}},Some(&verified_raw))?;
        }
        drop(verified_raw);
        cursor.last_raw = Some(raw.digest);
        if !replayed_overlap {
            store.checkpoint_daily(lease,&serde_json::to_string(&cursor)?,&json!({"page":evidence,"scan":cursor.scan,"dispositions":page.dispositions.iter().take(100).collect::<Vec<_>>(),"disposition_count":page.dispositions.len(),"dispositions_truncated":page.dispositions.len()>100}),now())?;
        }
        if cursor.scan.ended {
            return finish(
                store,
                lease,
                &cursor,
                if cursor.scan.complete() {
                    DayState::Complete
                } else {
                    DayState::Blocked
                },
                if cursor.scan.complete() {
                    "explicit_provider_end"
                } else {
                    "coverage_gaps"
                },
                json!({}),
                None,
            );
        }
        request_cursor = cursor.scan.next_cursor.clone();
        if cursor.scan.pages - initial_pages >= lease.config.daily.provider.max_pages {
            break;
        }
    }
    finish(
        store,
        lease,
        &cursor,
        DayState::Partial,
        "page_budget_checkpoint",
        json!({}),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::WatchAccount, jobs::Failure, network::Response, planner::DayState};
    use serde_json::{Value, json};
    use std::{collections::VecDeque, sync::Mutex};
    struct Fixture {
        pages: Mutex<VecDeque<(Option<String>, Response)>>,
    }
    impl Provider for Fixture {
        fn lookup<'a>(
            &'a self,
            _handle: &'a str,
            _cancel: &'a CancellationToken,
        ) -> futures_util::future::BoxFuture<'a, Result<Response, Failure>> {
            Box::pin(async { panic!("known ID needs no lookup") })
        }
        fn page<'a>(
            &'a self,
            _account: &'a str,
            cursor: Option<&'a str>,
            _cancel: &'a CancellationToken,
        ) -> futures_util::future::BoxFuture<'a, Result<Response, Failure>> {
            Box::pin(async move {
                let (expected, response) = self
                    .pages
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("bounded request count");
                assert_eq!(cursor, expected.as_deref());
                Ok(response)
            })
        }
    }
    fn response(entries: Vec<Value>, next: Option<&str>, end: bool) -> Response {
        let mut entries = entries;
        if let Some(cursor) = next {
            entries
                .push(json!({"entryId":"bottom","content":{"cursorType":"Bottom","value":cursor}}));
        }
        let mut instructions = vec![json!({"type":"TimelineAddEntries","entries":entries})];
        if end {
            instructions.push(json!({"type":"TimelineTerminateTimeline","direction":"Bottom"}));
        }
        Response{submitted:"https://x.com/fixture".parse().unwrap(),resolved:"https://x.com/fixture".parse().unwrap(),redirects:vec![],status:200,content_type:"application/json".into(),charset:None,retry_after_seconds:None,bytes:serde_json::to_vec(&json!({"data":{"user":{"result":{"timeline_v2":{"timeline":{"instructions":instructions}}}}}})).unwrap()}
    }
    fn entry(id: &str, time: &str) -> Value {
        json!({"entryId":format!("tweet-{id}"),"content":{"itemContent":{"tweet_results":{"result":{"rest_id":id,"legacy":{"user_id_str":"10","created_at":time}}}}}})
    }
    fn setup() -> (tempfile::TempDir, Store, Config, String) {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config {
            state_dir: root.path().join("state"),
            ..Config::default()
        };
        config.daily.enabled = true;
        config.daily.timezone = "UTC".into();
        config.daily.accounts = vec![WatchAccount {
            key: "fixture".into(),
            user_id: Some("10".into()),
            ..WatchAccount::default()
        }];
        config.daily.provider.max_pages = 1;
        config.daily.interval_seconds = 1;
        let store = Store::open(&config.state_dir).unwrap();
        let day = "1970-01-02".parse().unwrap();
        let report = store
            .backfill_daily(&config, day, day, crate::storage::now())
            .unwrap();
        let id = report.windows[0].id.clone();
        (root, store, config, id)
    }
    #[tokio::test]
    async fn pagination_reopens_with_overlap_and_retains_raw_activity_pins_without_delivery() {
        let (_root, store, config, id) = setup();
        let raw = response(
            vec![entry("100", "Fri Jan 02 00:00:10 +0000 1970")],
            Some("next"),
            false,
        );
        let bytes = raw.bytes.clone();
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([(None, raw)])),
        };
        let lease = store
            .claim_daily_window(&config, crate::storage::now())
            .unwrap()
            .unwrap();
        collect_lease(&store, &lease, &fixture, &CancellationToken::new())
            .await
            .unwrap();
        let before = store.daily_window(&id, crate::storage::now()).unwrap();
        assert_eq!(before.state, DayState::Partial);
        assert!(before.cursor.is_some());
        assert_eq!(
            store.read_blob(&crate::digest(&bytes), 10000).unwrap(),
            bytes
        );
        let reopened = Store::open(&config.state_dir).unwrap();
        reopened
            .connection()
            .unwrap()
            .execute(
                "UPDATE daily_windows SET next_attempt_at=0 WHERE id=?1",
                [&id],
            )
            .unwrap();
        let lease = reopened
            .claim_daily_window(&config, crate::storage::now())
            .unwrap()
            .unwrap();
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([
                (
                    None,
                    response(
                        vec![entry("100", "Fri Jan 02 00:00:10 +0000 1970")],
                        Some("next"),
                        false,
                    ),
                ),
                (
                    Some("next".into()),
                    response(
                        vec![entry("101", "Fri Jan 02 00:00:20 +0000 1970")],
                        None,
                        true,
                    ),
                ),
            ])),
        };
        collect_lease(&reopened, &lease, &fixture, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            reopened
                .daily_window(&id, crate::storage::now())
                .unwrap()
                .state,
            DayState::Complete
        );
        let db = reopened.connection().unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM activities", [], |r| r
                .get::<_, usize>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM daily_pages", [], |r| r
                .get::<_, usize>(0))
                .unwrap(),
            2
        );
        for table in ["outbox", "delivery_attempts", "jobs"] {
            assert_eq!(
                db.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r
                    .get::<_, usize>(0))
                    .unwrap(),
                0
            );
        }
    }
    #[tokio::test]
    async fn rate_limit_is_retained_and_another_account_can_progress() {
        let (_root, store, mut config, id) = setup();
        config.daily.accounts.push(WatchAccount {
            key: "other".into(),
            user_id: Some("20".into()),
            ..WatchAccount::default()
        });
        let day = "1970-01-02".parse().unwrap();
        store
            .backfill_daily(&config, day, day, crate::storage::now())
            .unwrap();
        let mut raw = response(vec![], None, false);
        raw.status = 429;
        raw.retry_after_seconds = Some(31);
        raw.bytes = br#"{"errors":[{"code":88}]}"#.to_vec();
        let lease = store
            .claim_daily_window(&config, crate::storage::now())
            .unwrap()
            .unwrap();
        let claimed = lease.window.plan.id.clone();
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([(None, raw)])),
        };
        collect_lease(&store, &lease, &fixture, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            store
                .daily_window(&claimed, crate::storage::now())
                .unwrap()
                .state,
            DayState::Blocked
        );
        let next = store
            .claim_daily_window(&config, crate::storage::now())
            .unwrap()
            .unwrap();
        assert_ne!(next.window.plan.id, claimed);
        let _ = id;
        assert!(
            store
                .connection()
                .unwrap()
                .query_row(
                    "SELECT next_attempt_at FROM daily_windows WHERE id=?1",
                    [claimed],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap()
                >= crate::storage::now() + 30
        );
    }
    #[tokio::test]
    async fn configured_worker_entry_claims_finishes_and_hold_suppresses_provider() {
        let (_root, store, config, id) = setup();
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([(None, response(vec![], None, true))])),
        };
        assert!(
            tick(&store, &config, &fixture, &CancellationToken::new())
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .daily_window(&id, crate::storage::now())
                .unwrap()
                .state,
            DayState::Complete
        );
        store
            .connection()
            .unwrap()
            .execute("INSERT INTO operational_holds VALUES('fixture','hold')", [])
            .unwrap();
        assert!(
            !tick(&store, &config, &fixture, &CancellationToken::new())
                .await
                .unwrap()
        );
        assert!(
            !step(&store, &config, &CancellationToken::new())
                .await
                .unwrap()
        );
    }

    struct LookupFixture(Fixture);
    impl Provider for LookupFixture {
        fn lookup<'a>(
            &'a self,
            handle: &'a str,
            _cancel: &'a CancellationToken,
        ) -> futures_util::future::BoxFuture<'a, Result<Response, Failure>> {
            Box::pin(async move {
                assert_eq!(handle, "fixture");
                let mut raw = response(vec![], None, true);
                raw.bytes = br#"{"data":{"user":{"result":{"rest_id":"10"}}}}"#.to_vec();
                Ok(raw)
            })
        }
        fn page<'a>(
            &'a self,
            account: &'a str,
            cursor: Option<&'a str>,
            cancel: &'a CancellationToken,
        ) -> futures_util::future::BoxFuture<'a, Result<Response, Failure>> {
            assert_eq!(account, "10");
            self.0.page(account, cursor, cancel)
        }
    }
    #[tokio::test]
    async fn lookup_binds_strong_identity_and_pins_raw_before_empty_day_completion() {
        let (root, _unused, mut config, _) = setup();
        config.state_dir = root.path().join("lookup");
        config.daily.accounts[0].user_id = None;
        config.daily.accounts[0].handle = Some("fixture".into());
        let store = Store::open(&config.state_dir).unwrap();
        let day = "1970-01-02".parse().unwrap();
        let id = store
            .backfill_daily(&config, day, day, crate::storage::now())
            .unwrap()
            .windows[0]
            .id
            .clone();
        let fixture = LookupFixture(Fixture {
            pages: Mutex::new(VecDeque::from([(None, response(vec![], None, true))])),
        });
        assert!(
            tick(&store, &config, &fixture, &CancellationToken::new())
                .await
                .unwrap()
        );
        let db = store.connection().unwrap();
        let (identity, evidence): (String, String) = db
            .query_row(
                "SELECT identity,evidence FROM daily_account_resolutions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&identity).unwrap()["user_id"],
            "10"
        );
        let evidence: Value = serde_json::from_str(&evidence).unwrap();
        store
            .verify_blob(evidence["raw_digest"].as_str().unwrap())
            .unwrap();
        assert_eq!(
            store
                .daily_window(&id, crate::storage::now())
                .unwrap()
                .state,
            DayState::Complete
        );
    }
    #[tokio::test]
    async fn interrupted_next_page_preserves_checkpoint_and_replays_overlap_after_reopen() {
        let (_root, store, mut config, id) = setup();
        config.daily.provider.max_pages = 2;
        let mut failure = response(vec![], None, false);
        failure.status = 503;
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([
                (
                    None,
                    response(
                        vec![entry("100", "Fri Jan 02 00:00:10 +0000 1970")],
                        Some("next"),
                        false,
                    ),
                ),
                (Some("next".into()), failure),
            ])),
        };
        assert!(
            tick(&store, &config, &fixture, &CancellationToken::new())
                .await
                .unwrap()
        );
        assert_eq!(
            store
                .daily_window(&id, crate::storage::now())
                .unwrap()
                .state,
            DayState::Blocked
        );
        let reopened = Store::open(&config.state_dir).unwrap();
        reopened
            .retry_daily_window(&config, &id, crate::storage::now())
            .unwrap();
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([
                (
                    None,
                    response(
                        vec![entry("100", "Fri Jan 02 00:00:10 +0000 1970")],
                        Some("next"),
                        false,
                    ),
                ),
                (Some("next".into()), response(vec![], None, true)),
            ])),
        };
        assert!(
            tick(&reopened, &config, &fixture, &CancellationToken::new())
                .await
                .unwrap()
        );
        assert_eq!(
            reopened
                .daily_window(&id, crate::storage::now())
                .unwrap()
                .state,
            DayState::Complete
        );
        assert_eq!(
            reopened
                .connection()
                .unwrap()
                .query_row("SELECT count(*) FROM activities", [], |r| r
                    .get::<_, usize>(0))
                .unwrap(),
            1
        );
    }
    #[tokio::test]
    async fn changed_overlap_keeps_page_identity_and_total_budget_is_resumable() {
        let (_root, store, mut config, id) = setup();
        config.daily.provider.max_scan_pages = 2;
        let first = response(
            vec![entry("100", "Fri Jan 02 00:00:10 +0000 1970")],
            Some("next"),
            false,
        );
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([(None, first)])),
        };
        tick(&store, &config, &fixture, &CancellationToken::new())
            .await
            .unwrap();
        store
            .connection()
            .unwrap()
            .execute("UPDATE daily_windows SET next_attempt_at=0", [])
            .unwrap();
        let changed = response(
            vec![
                entry("100", "Fri Jan 02 00:00:10 +0000 1970"),
                entry("101", "Fri Jan 02 00:00:11 +0000 1970"),
            ],
            Some("next"),
            false,
        );
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([
                (None, changed),
                (Some("next".into()), response(vec![], Some("last"), false)),
            ])),
        };
        tick(&store, &config, &fixture, &CancellationToken::new())
            .await
            .unwrap();
        let window = store.daily_window(&id, now()).unwrap();
        let cursor: Cursor = serde_json::from_str(window.cursor.as_ref().unwrap()).unwrap();
        assert_eq!(cursor.scan.pages, 2);
        assert_eq!(cursor.scan.overlap_cursor.as_deref(), Some("next"));
        assert_eq!(cursor.scan.next_cursor.as_deref(), Some("last"));
        assert!(cursor.scan.gaps.contains("overlap_page_changed"));
        assert_eq!(
            store
                .connection()
                .unwrap()
                .query_row("SELECT count(*) FROM activities", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
        store
            .connection()
            .unwrap()
            .execute("UPDATE daily_windows SET next_attempt_at=0", [])
            .unwrap();
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::new()),
        };
        tick(&store, &config, &fixture, &CancellationToken::new())
            .await
            .unwrap();
        let window = store.daily_window(&id, now()).unwrap();
        assert_eq!(window.state, DayState::Blocked);
        assert_eq!(window.reason.as_deref(), Some("provider_scan_budget"));
        assert_eq!(
            window.cursor.unwrap(),
            serde_json::to_string(&cursor).unwrap()
        );
        config.daily.provider.max_scan_pages = 3;
        store.retry_daily_window(&config, &id, now()).unwrap();
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([
                (Some("next".into()), response(vec![], Some("last"), false)),
                (Some("last".into()), response(vec![], None, true)),
            ])),
        };
        tick(&store, &config, &fixture, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            store.daily_window(&id, now()).unwrap().reason.as_deref(),
            Some("coverage_gaps")
        );
    }
    #[tokio::test]
    async fn many_activities_verify_one_shared_raw_page_only_once() {
        let (_root, store, config, id) = setup();
        let entries = (100..132)
            .map(|n| entry(&n.to_string(), "Fri Jan 02 00:00:10 +0000 1970"))
            .collect();
        let mut page = response(entries, None, true);
        let mut value: Value = serde_json::from_slice(&page.bytes).unwrap();
        value["ignored_provider_payload"] = json!("x".repeat(1_000_000));
        page.bytes = serde_json::to_vec(&value).unwrap();
        let bytes = page.bytes.len() as u64;
        let before = store.verification_bytes();
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([(None, page)])),
        };
        tick(&store, &config, &fixture, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            store.daily_window(&id, now()).unwrap().state,
            DayState::Complete
        );
        assert_eq!(
            store
                .connection()
                .unwrap()
                .query_row("SELECT count(*) FROM activities", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            32
        );
        let verified = store.verification_bytes() - before;
        println!(
            "shared-page logical verification bytes: {verified}; page bytes: {bytes}; activities: 32"
        );
        assert!(
            verified <= 3 * bytes,
            "page bytes {bytes}, actual bytes read by verification {verified}"
        );
    }
    #[tokio::test]
    async fn maximum_provider_cursor_and_seen_set_checkpoint_resume_without_loss() {
        let (_root, store, mut config, id) = setup();
        config.daily.provider.max_scan_pages = 512;
        let overlap = "\u{1}".repeat(16384);
        let next = "\u{2}".repeat(16384);
        let page = response(vec![], Some(&next), false);
        assert_eq!(
            parse_page("10", &page.bytes, 100)
                .unwrap()
                .next_cursor
                .as_deref(),
            Some(next.as_str())
        );
        assert_eq!(
            super::super::fx::parse(
                "10",
                &json!({"code":200,"results":[],"cursor":{"bottom":next}}),
                100
            )
            .unwrap()
            .next_cursor
            .as_deref(),
            Some(next.as_str())
        );
        let raw = store.publish_bytes(&page.bytes).unwrap();
        let mut seen = (0..510)
            .map(|n| crate::digest(n.to_string()))
            .collect::<std::collections::BTreeSet<_>>();
        seen.insert(crate::digest(&next));
        let cursor = Cursor {
            version: 1,
            provider: "x-web-session-v1".into(),
            last_raw: Some(raw.digest),
            overlap_raw: None,
            scan: Scan {
                pages: 511,
                next_cursor: Some(next.clone()),
                overlap_cursor: Some(overlap.clone()),
                seen_cursors: seen,
                ..Scan::default()
            },
        };
        let encoded = serde_json::to_string(&cursor).unwrap();
        assert!(encoded.len() > 131072);
        assert!(encoded.len() < 262144);
        let lease = store.claim_daily_window(&config, now()).unwrap().unwrap();
        store
            .checkpoint_daily(
                &lease,
                &encoded,
                &json!({"fixture":"maximum_aggregate_cursor"}),
                now(),
            )
            .expect("accepted provider cursor bounds must fit aggregate checkpoint");
        store
            .finish_daily_window(
                &lease,
                DailyProgress {
                    state: DayState::Partial,
                    cursor: Some(encoded),
                    reason: Some("fixture_checkpoint".into()),
                    evidence: json!({}),
                    coverage_complete: false,
                },
                now(),
            )
            .unwrap();
        store
            .connection()
            .unwrap()
            .execute("UPDATE daily_windows SET next_attempt_at=0", [])
            .unwrap();
        drop(store);
        let store = Store::open(&config.state_dir).unwrap();
        let fixture = Fixture {
            pages: Mutex::new(VecDeque::from([
                (Some(overlap), page),
                (Some(next), response(vec![], None, true)),
            ])),
        };
        tick(&store, &config, &fixture, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            store.daily_window(&id, now()).unwrap().state,
            DayState::Complete
        );
    }
}
