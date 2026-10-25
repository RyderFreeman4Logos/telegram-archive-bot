//! FxEmbed v2 public API; no account, cookie, token, or translation request.
use super::{DiscoveredActivity, Disposition, Provider, TimelinePage};
use crate::{
    activities::{ActivityKind, TimeEvidence, TimePrecision, Timestamp},
    jobs::Failure,
    network::{PublicHttp, Response},
    sources::{Identity, Related, RelationKind},
};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct FxTimeline {
    pub http: PublicHttp,
    pub page_size: usize,
    pub max_bytes: u64,
    pub timeout: Duration,
    #[cfg(test)]
    pub(crate) base: &'static str,
}
impl FxTimeline {
    async fn request(
        &self,
        account: &str,
        statuses: bool,
        cursor: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Response, Failure> {
        if account.is_empty()
            || account.len() > 64
            || !account
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b':')
            || !(1..=100).contains(&self.page_size)
            || self.max_bytes == 0
            || self.timeout.is_zero()
            || cursor.is_some_and(|s| s.is_empty() || s.len() > 16384)
        {
            return Err(Failure::InvalidInput);
        }
        #[cfg(test)]
        let base = self.base;
        #[cfg(not(test))]
        let base = "https://api.fxtwitter.com";
        let suffix = if statuses { "/statuses" } else { "" };
        let mut url = url::Url::parse(&format!("{base}/2/profile/{account}{suffix}"))
            .map_err(|_| Failure::InvalidInput)?;
        if statuses {
            url.query_pairs_mut()
                .append_pair("count", &self.page_size.to_string())
                .append_pair("with_replies", "1");
            if let Some(cursor) = cursor {
                url.query_pairs_mut().append_pair("cursor", cursor);
            }
        }
        self.http
            .request(
                url.as_str(),
                reqwest::Method::GET,
                reqwest::header::HeaderMap::new(),
                self.max_bytes,
                self.timeout,
                cancel,
            )
            .await
    }
}
impl Provider for FxTimeline {
    fn name(&self) -> &'static str {
        "fx-twitter-v2"
    }
    fn lookup<'a>(
        &'a self,
        handle: &'a str,
        cancel: &'a CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<Response, Failure>> {
        Box::pin(async move {
            if handle.len() > 15
                || !handle
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            {
                return Err(Failure::InvalidInput);
            }
            self.request(handle, false, None, cancel).await
        })
    }
    fn page<'a>(
        &'a self,
        account: &'a str,
        cursor: Option<&'a str>,
        cancel: &'a CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<Response, Failure>> {
        Box::pin(async move {
            Identity::x(account).map_err(|_| Failure::InvalidInput)?;
            self.request(&format!("id:{account}"), true, cursor, cancel)
                .await
        })
    }
}

pub(super) fn parse(
    account: &str,
    value: &Value,
    max_entries: usize,
) -> Result<TimelinePage, Failure> {
    if value["code"] != 200 {
        return Err(Failure::InvalidOutput);
    }
    let rows = value["results"]
        .as_array()
        .filter(|r| r.len() <= max_entries)
        .ok_or(Failure::InvalidOutput)?;
    let bottom = value
        .pointer("/cursor/bottom")
        .ok_or(Failure::InvalidOutput)?;
    let next_cursor = if bottom.is_null() {
        None
    } else {
        Some(
            bottom
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 16384)
                .ok_or(Failure::InvalidOutput)?
                .to_owned(),
        )
    };
    let mut page = TimelinePage {
        activities: vec![],
        dispositions: vec![],
        explicit_end: next_cursor.is_none(),
        next_cursor,
    };
    for (index, row) in rows.iter().enumerate() {
        let entry_id = format!("fx-row:{index}");
        let Some(id) = row["id"].as_str().filter(|s| Identity::x(s).is_ok()) else {
            page.dispositions.push(Disposition {
                entry_id,
                reason: "unavailable_source_identity".into(),
            });
            continue;
        };
        let repost = row.get("reposted_by").is_some_and(|v| !v.is_null());
        let actor = if repost {
            row.pointer("/reposted_by/id")
        } else {
            row.pointer("/author/id")
        }
        .and_then(Value::as_str);
        if actor != Some(account) {
            page.dispositions.push(Disposition {
                entry_id,
                reason: if actor.is_some() {
                    "context_actor"
                } else {
                    "unknown_actor"
                }
                .into(),
            });
            continue;
        }
        let mut relations = vec![];
        for (pointer, kind) in [
            ("/replying_to/status", RelationKind::RepliesTo),
            ("/quote/id", RelationKind::Quotes),
        ] {
            if let Some(target) = row.pointer(pointer).and_then(Value::as_str) {
                if let Ok(target) = Identity::x(target) {
                    relations.push(Related { target, kind });
                } else {
                    page.dispositions.push(Disposition {
                        entry_id: entry_id.clone(),
                        reason: "invalid_relation_identity".into(),
                    });
                }
            }
        }
        let kind = if repost {
            ActivityKind::Repost
        } else if row
            .pointer("/replying_to/status")
            .is_some_and(Value::is_string)
        {
            ActivityKind::Reply
        } else if row.get("quote").is_some_and(|v| !v.is_null()) {
            ActivityKind::Quote
        } else {
            ActivityKind::Original
        };
        let provenance = json!({"provider":"fx-twitter-v2","field":"created_timestamp","raw":row["created_timestamp"]});
        let source_time = match row["created_timestamp"].as_i64().filter(|t| *t >= 0) {
            Some(t) => TimeEvidence::Instant {
                timestamp: Timestamp::seconds(t),
                precision: TimePrecision::Second,
                provenance,
            },
            None => super::time(&row["created_at"]),
        };
        // The API exposes the original post's time, never the repost event's time.
        let activity_time = if repost {
            TimeEvidence::Unknown {
                provenance: json!({"provider":"fx-twitter-v2","reason":"repost_event_time_not_exposed"}),
            }
        } else {
            source_time.clone()
        };
        page.activities.push(DiscoveredActivity {
            provider_event_id: if repost { None } else { Some(id.into()) },
            actor_id: actor.map(str::to_owned),
            source_id: id.into(),
            kind,
            activity_time,
            source_time,
            entry_id,
            pinned: false,
            provenance: json!({"provider":"fx-twitter-v2","repost_event_identity_exposed":false}),
            relations,
        });
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{Config, DailyProviderKind, WatchAccount},
        planner::DayState,
        storage::{Store, now},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn anonymous_http_lookup_cursor_and_store_resume_need_no_secrets() {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config {
            state_dir: root.path().join("state"),
            ..Config::default()
        };
        config.daily.enabled = true;
        config.daily.timezone = "UTC".into();
        config.daily.provider.kind = DailyProviderKind::FxTwitter;
        config.daily.provider.max_pages = 1;
        config.daily.accounts = vec![WatchAccount {
            key: "fixture".into(),
            handle: Some("fixture".into()),
            ..WatchAccount::default()
        }];
        config.validate().unwrap();
        assert_eq!(
            super::super::readiness(&config),
            "anonymous_configured_unverified"
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let first=json!({"code":200,"results":[{"id":"100","author":{"id":"10"},"created_timestamp":86401}],"cursor":{"bottom":"next+/="}}).to_string();
        let last=json!({"code":200,"results":[{"id":"200","author":{"id":"20"},"reposted_by":{"id":"10"},"created_timestamp":100}],"cursor":{"bottom":null}}).to_string();
        let server = tokio::spawn(async move {
            for (index, body) in [
                r#"{"code":200,"user":{"id":"10"}}"#.to_owned(),
                first.clone(),
                {
                    let mut changed: Value = serde_json::from_str(&first).unwrap();
                    changed["results"][0]["views"] = json!(77);
                    changed.to_string()
                },
                last,
            ]
            .into_iter()
            .enumerate()
            {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let mut b = [0; 2048];
                    let n = socket.read(&mut b).await.unwrap();
                    assert!(n > 0 && request.len() + n < 32768);
                    request.extend_from_slice(&b[..n]);
                }
                let request = String::from_utf8(request).unwrap();
                let lower = request.to_ascii_lowercase();
                assert!(
                    !lower.contains("authorization:")
                        && !lower.contains("cookie:")
                        && !lower.contains("x-csrf-token:")
                );
                let path = request
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap();
                let url = url::Url::parse(&format!("http://fixture.invalid{path}")).unwrap();
                if index == 0 {
                    assert_eq!(url.path(), "/2/profile/fixture");
                } else {
                    assert_eq!(url.path(), "/2/profile/id:10/statuses");
                    let query = url
                        .query_pairs()
                        .collect::<std::collections::BTreeMap<_, _>>();
                    assert_eq!(query["with_replies"], "1");
                    assert!(!query.contains_key("lang") && !query.contains_key("since"));
                    assert_eq!(
                        query.get("cursor").map(|s| s.as_ref()),
                        if index == 3 { Some("next+/=") } else { None }
                    );
                }
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        let provider = FxTimeline {
            http: PublicHttp::fixture_to(addr),
            page_size: 20,
            max_bytes: 10000,
            timeout: Duration::from_secs(3),
            base: "http://fixture.invalid",
        };
        let store = Store::open(&config.state_dir).unwrap();
        let day = "1970-01-02".parse().unwrap();
        let id = store
            .backfill_daily(&config, day, day, now())
            .unwrap()
            .windows[0]
            .id
            .clone();
        super::super::tick(&store, &config, &provider, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            store.daily_window(&id, now()).unwrap().state,
            DayState::Partial
        );
        store
            .connection()
            .unwrap()
            .execute("UPDATE daily_windows SET next_attempt_at=0", [])
            .unwrap();
        drop(store);
        let store = Store::open(&config.state_dir).unwrap();
        super::super::tick(&store, &config, &provider, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            store.daily_window(&id, now()).unwrap().state,
            DayState::Blocked
        );
        let db = store.connection().unwrap();
        assert_eq!(
            db.query_row("SELECT count(*) FROM activities", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM jobs", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM outbox", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
    }
}
