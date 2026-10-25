//! DA03 / #70: free account timeline discovery, separate from known-URL hydration.
use crate::{
    activities::{ActivityKind, TimeEvidence, TimePrecision, Timestamp},
    jobs::Failure,
    sources::Identity,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[path = "timeline_web.rs"]
mod web;
pub use web::WebTimeline;
#[path = "timeline_fx.rs"]
mod fx;
pub use fx::FxTimeline;
#[path = "timeline_collect.rs"]
mod collect;
pub use collect::{collect_lease, step, tick};

pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str {
        "x-web-session-v1"
    }
    fn lookup<'a>(
        &'a self,
        handle: &'a str,
        cancel: &'a tokio_util::sync::CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<crate::network::Response, Failure>>;
    fn page<'a>(
        &'a self,
        account: &'a str,
        cursor: Option<&'a str>,
        cancel: &'a tokio_util::sync::CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<crate::network::Response, Failure>>;
}
impl Provider for WebTimeline {
    fn lookup<'a>(
        &'a self,
        handle: &'a str,
        cancel: &'a tokio_util::sync::CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<crate::network::Response, Failure>> {
        Box::pin(self.lookup(handle, cancel))
    }
    fn page<'a>(
        &'a self,
        account: &'a str,
        cursor: Option<&'a str>,
        cancel: &'a tokio_util::sync::CancellationToken,
    ) -> futures_util::future::BoxFuture<'a, Result<crate::network::Response, Failure>> {
        Box::pin(self.fetch(account, cursor, cancel))
    }
}

pub fn readiness(config: &crate::config::Config) -> &'static str {
    use crate::config::DailyProviderKind;
    if !config.daily.enabled || config.daily.provider.kind == DailyProviderKind::Disabled {
        return "disabled";
    }
    if config.daily.provider.kind == DailyProviderKind::FxTwitter {
        return "anonymous_configured_unverified";
    }
    if config
        .daily
        .provider
        .cookie
        .as_ref()
        .is_none_or(|r| r.read().is_err())
        || config
            .daily
            .provider
            .bearer
            .as_ref()
            .is_none_or(|r| r.read().is_err())
    {
        return "pending_credentials";
    }
    "configured_unverified"
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveredActivity {
    pub provider_event_id: Option<String>,
    pub actor_id: Option<String>,
    pub source_id: String,
    pub kind: ActivityKind,
    pub activity_time: TimeEvidence,
    pub source_time: TimeEvidence,
    pub entry_id: String,
    pub pinned: bool,
    pub provenance: Value,
    pub relations: Vec<crate::sources::Related>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Disposition {
    pub entry_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TimelinePage {
    pub activities: Vec<DiscoveredActivity>,
    pub dispositions: Vec<Disposition>,
    pub next_cursor: Option<String>,
    pub explicit_end: bool,
}

pub fn parse_page(account: &str, raw: &[u8], max_entries: usize) -> Result<TimelinePage, Failure> {
    Identity::x(account).map_err(|_| Failure::InvalidInput)?;
    if max_entries == 0 || max_entries > 10000 {
        return Err(Failure::InvalidInput);
    }
    let value: Value = serde_json::from_slice(raw).map_err(|_| Failure::InvalidOutput)?;
    if value.get("code").is_some() {
        return fx::parse(account, &value, max_entries);
    }
    let instructions = value
        .pointer("/data/user/result/timeline_v2/timeline/instructions")
        .or_else(|| value.pointer("/data/user/result/timeline/timeline/instructions"))
        .and_then(Value::as_array)
        .ok_or(Failure::InvalidOutput)?;
    let mut page = TimelinePage {
        activities: vec![],
        dispositions: vec![],
        next_cursor: None,
        explicit_end: false,
    };
    let mut entries = std::collections::VecDeque::new();
    if instructions.len() > max_entries {
        return Err(Failure::InvalidOutput);
    }
    for (i, instruction) in instructions.iter().enumerate() {
        match instruction["type"].as_str() {
            Some("TimelineTerminateTimeline") if instruction["direction"] == "Bottom" => {
                page.explicit_end = true
            }
            Some("TimelineAddEntries") => {
                let items = instruction["entries"]
                    .as_array()
                    .ok_or(Failure::InvalidOutput)?;
                if entries.len().saturating_add(items.len()) > max_entries {
                    return Err(Failure::InvalidOutput);
                }
                entries.extend(items.iter().map(|v| (v, false, 0)));
            }
            Some("TimelinePinEntry" | "TimelineReplaceEntry") => {
                entries.push_back((
                    &instruction["entry"],
                    instruction["type"] == "TimelinePinEntry",
                    0,
                ));
            }
            Some("TimelineAddToModule") => {
                let items = instruction["moduleItems"]
                    .as_array()
                    .ok_or(Failure::InvalidOutput)?;
                if entries.len().saturating_add(items.len()) > max_entries {
                    return Err(Failure::InvalidOutput);
                }
                entries.extend(items.iter().map(|v| (v, false, 1)));
            }
            Some("TimelineTerminateTimeline") => {}
            _ => page.dispositions.push(Disposition {
                entry_id: format!("instruction:{i}"),
                reason: "unrecognized_instruction".into(),
            }),
        }
    }
    let mut visited = 0usize;
    while let Some((entry, pinned, depth)) = entries.pop_front() {
        visited += 1;
        if visited > max_entries || depth > 4 {
            return Err(Failure::InvalidOutput);
        }
        let id = entry["entryId"]
            .as_str()
            .filter(|id| !id.is_empty() && id.len() <= 512)
            .ok_or(Failure::InvalidOutput)?;
        let content = entry
            .get("content")
            .or_else(|| entry.get("item"))
            .ok_or(Failure::InvalidOutput)?;
        if content["cursorType"] == "Bottom" {
            let cursor = content["value"]
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 16384)
                .ok_or(Failure::InvalidOutput)?;
            if page
                .next_cursor
                .as_deref()
                .is_some_and(|prior| prior != cursor)
            {
                return Err(Failure::InvalidOutput);
            }
            page.next_cursor = Some(cursor.into());
            continue;
        }
        if content["cursorType"] == "Top" {
            continue;
        }
        if let Some(items) = content["items"].as_array() {
            if visited
                .saturating_add(entries.len())
                .saturating_add(items.len())
                > max_entries
            {
                return Err(Failure::InvalidOutput);
            }
            // Keep provider order, including conversation modules.
            for item in items.iter().rev() {
                entries.push_front((item, pinned, depth + 1));
            }
            continue;
        }
        let item = &content["itemContent"];
        let mut omit = |reason: &str| {
            page.dispositions.push(Disposition {
                entry_id: id.into(),
                reason: reason.into(),
            })
        };
        if item.get("promotedMetadata").is_some_and(|v| !v.is_null()) {
            omit("promoted");
            continue;
        }
        let tweet = &item["tweet_results"]["result"];
        let tweet = tweet.get("tweet").unwrap_or(tweet);
        let Some(event_id) = tweet["rest_id"].as_str() else {
            omit("unrecognized_or_unavailable_item");
            continue;
        };
        Identity::x(event_id).map_err(|_| Failure::InvalidOutput)?;
        let author = tweet["legacy"]["user_id_str"].as_str().or_else(|| {
            tweet
                .pointer("/core/user_results/result/rest_id")
                .and_then(Value::as_str)
        });
        if author != Some(account) {
            omit(if author.is_some() {
                "context_actor"
            } else {
                "unknown_actor"
            });
            continue;
        }
        let retweet = tweet
            .pointer("/legacy/retweeted_status_result/result")
            .or_else(|| tweet.pointer("/retweeted_status_result/result"))
            .or_else(|| tweet.pointer("/legacy/repostedStatusResults/result"))
            .or_else(|| tweet.pointer("/repostedStatusResults/result"))
            .filter(|v| !v.is_null());
        let target_id = tweet["legacy"]["retweeted_status_id_str"].as_str();
        let is_repost = retweet.is_some() || target_id.is_some();
        let source = if is_repost {
            retweet
                .map(|v| v.get("tweet").unwrap_or(v))
                .unwrap_or(&Value::Null)
        } else {
            tweet
        };
        let Some(source_id) = source["rest_id"].as_str().or(target_id) else {
            omit("unavailable_repost_target_identity");
            continue;
        };
        Identity::x(source_id).map_err(|_| Failure::InvalidOutput)?;
        let kind = if is_repost {
            ActivityKind::Repost
        } else if tweet["legacy"]["in_reply_to_status_id_str"].is_string() {
            ActivityKind::Reply
        } else if tweet["legacy"]["quoted_status_id_str"].is_string() {
            ActivityKind::Quote
        } else {
            ActivityKind::Original
        };
        let mut relations = vec![];
        for (field, kind) in [
            (
                "in_reply_to_status_id_str",
                crate::sources::RelationKind::RepliesTo,
            ),
            ("quoted_status_id_str", crate::sources::RelationKind::Quotes),
        ] {
            if let Some(target) = source["legacy"][field].as_str() {
                relations.push(crate::sources::Related {
                    target: Identity::x(target).map_err(|_| Failure::InvalidOutput)?,
                    kind,
                });
            }
        }
        page.activities.push(DiscoveredActivity {
            provider_event_id: Some(event_id.into()),
            actor_id: author.map(str::to_owned),
            source_id: source_id.into(),
            kind,
            activity_time: time(&tweet["legacy"]["created_at"]),
            source_time: time(&source["legacy"]["created_at"]),
            entry_id: id.into(),
            pinned,
            relations,
            provenance: json!({"entry_id":id,"provider":"x-web-session-v1"}),
        });
    }
    Ok(page)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scan {
    pub next_cursor: Option<String>,
    pub overlap_cursor: Option<String>,
    pub pages: usize,
    pub seen_cursors: std::collections::BTreeSet<String>,
    pub gaps: std::collections::BTreeSet<String>,
    pub selected: u64,
    pub outside_window: u64,
    pub uncertain_time: u64,
    pub ended: bool,
}
impl Scan {
    pub fn observe(
        &mut self,
        page: &TimelinePage,
        start: i64,
        end: i64,
    ) -> Result<Vec<usize>, Failure> {
        if start >= end || self.ended || self.pages >= 512 {
            return Err(Failure::InvalidInput);
        }
        if !page.explicit_end && page.next_cursor.is_none() {
            self.gaps.insert("missing_termination_evidence".into());
            return Err(Failure::InvalidOutput);
        }
        if let Some(cursor) = &page.next_cursor
            && !self.seen_cursors.insert(crate::digest(cursor))
        {
            self.gaps.insert("cursor_cycle".into());
            return Err(Failure::InvalidOutput);
        }
        self.overlap_cursor = self.next_cursor.clone();
        self.next_cursor = if page.explicit_end {
            None
        } else {
            page.next_cursor.clone()
        };
        self.pages += 1;
        self.ended = page.explicit_end;
        for skipped in &page.dispositions {
            if !matches!(skipped.reason.as_str(), "promoted" | "context_actor") {
                self.gaps.insert(skipped.reason.clone());
            }
        }
        let lower = Timestamp::seconds(start);
        let upper = Timestamp::seconds(end);
        let mut selected = vec![];
        for (index, activity) in page.activities.iter().enumerate() {
            let membership = match &activity.activity_time {
                TimeEvidence::Instant { timestamp, .. } => {
                    Some(lower <= *timestamp && *timestamp < upper)
                }
                TimeEvidence::Interval {
                    earliest, latest, ..
                } if *latest < lower || *earliest >= upper => Some(false),
                TimeEvidence::Interval {
                    earliest, latest, ..
                } if *earliest >= lower && *latest < upper => Some(true),
                _ => None,
            };
            match membership {
                Some(false) => self.outside_window += 1,
                Some(true) => {
                    self.selected += 1;
                    selected.push(index);
                }
                None => {
                    self.uncertain_time += 1;
                    self.gaps.insert("unknown_activity_time".into());
                    selected.push(index);
                }
            }
        }
        Ok(selected)
    }
    pub fn complete(&self) -> bool {
        self.ended && self.gaps.is_empty()
    }
}

fn time(value: &Value) -> TimeEvidence {
    let provenance = json!({"field":"created_at","raw":value});
    if let Some(s) = value.as_str()
        && let Ok(parsed) = chrono::DateTime::parse_from_str(s, "%a %b %d %H:%M:%S %z %Y")
    {
        return TimeEvidence::Instant {
            timestamp: Timestamp::seconds(parsed.timestamp()),
            precision: TimePrecision::Second,
            provenance,
        };
    }
    TimeEvidence::Unknown { provenance }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activities::{TimePrecision, Timestamp};
    use serde_json::json;

    fn tweet(id: &str, author: &str, date: &str) -> Value {
        json!({"__typename":"Tweet","rest_id":id,"legacy":{"id_str":id,"user_id_str":author,"full_text":"fixture body","created_at":date},"core":{"user_results":{"result":{"rest_id":author}}}})
    }
    fn entry(id: &str, result: Value) -> Value {
        json!({"entryId":id,"content":{"entryType":"TimelineTimelineItem","itemContent":{"itemType":"TimelineTweet","tweet_results":{"result":result}}}})
    }
    fn page(entries: Vec<Value>) -> Vec<u8> {
        serde_json::to_vec(&json!({"data":{"user":{"result":{"timeline_v2":{"timeline":{"instructions":[{"type":"TimelineAddEntries","entries":entries}]}}}}}})).unwrap()
    }
    #[test]
    fn free_timeline_preserves_activity_identity_and_repost_time() {
        let old = "Thu Jan 01 00:01:40 +0000 1970";
        let recent = "Thu Jan 01 00:16:40 +0000 1970";
        let original = tweet("100", "10", old);
        let mut quote = tweet("200", "10", recent);
        quote["legacy"]["quoted_status_id_str"] = json!("100");
        quote["quoted_status_result"] = json!({"result":original});
        let mut reply = tweet("300", "10", recent);
        reply["legacy"]["in_reply_to_status_id_str"] = json!("100");
        let mut repost = tweet("400", "10", recent);
        repost["legacy"]["retweeted_status_result"] = json!({"result":tweet("50","99",old)});
        let mut ad = entry("promoted-1", tweet("999", "10", recent));
        ad["content"]["itemContent"]["promotedMetadata"] = json!({"advertiser_results":{}});
        let raw = page(vec![
            entry("tweet-100", original),
            entry("tweet-200", quote),
            entry("tweet-300", reply),
            entry("tweet-400", repost),
            ad,
            json!({"entryId":"cursor-bottom","content":{"cursorType":"Bottom","value":"next-page"}}),
        ]);
        let result =
            parse_page("10", &raw, 100).expect("free timeline must enumerate account activities");
        assert_eq!(result.activities.len(), 4);
        assert_eq!(
            result.activities.iter().map(|a| a.kind).collect::<Vec<_>>(),
            vec![
                ActivityKind::Original,
                ActivityKind::Quote,
                ActivityKind::Reply,
                ActivityKind::Repost
            ]
        );
        assert_eq!(result.activities[1].relations.len(), 1);
        assert_eq!(
            result.activities[1].relations[0].kind,
            crate::sources::RelationKind::Quotes
        );
        assert_eq!(
            result.activities[2].relations[0].kind,
            crate::sources::RelationKind::RepliesTo
        );
        assert_eq!(result.activities[2].relations[0].target.external_id, "100");
        let repost = &result.activities[3];
        assert_eq!(repost.provider_event_id.as_deref(), Some("400"));
        assert_eq!(repost.source_id, "50");
        assert!(
            matches!(&repost.activity_time,TimeEvidence::Instant{timestamp,precision:TimePrecision::Second,..} if *timestamp==Timestamp::seconds(1000))
        );
        assert!(
            matches!(&repost.source_time,TimeEvidence::Instant{timestamp,..} if *timestamp==Timestamp::seconds(100))
        );
        assert_eq!(result.next_cursor.as_deref(), Some("next-page"));
        assert!(!result.explicit_end);
        assert_eq!(result.dispositions.len(), 1);
        assert_eq!(result.dispositions[0].reason, "promoted");
    }

    #[test]
    fn free_timeline_pins_modules_and_unknown_items_remain_accounted() {
        let old = "Thu Jan 01 00:01:40 +0000 1970";
        let mut pinned = entry("tweet-100", tweet("100", "10", old));
        pinned["content"]["itemContent"]["socialContext"] =
            json!({"contextType":"TimelineGeneralContext","text":"Pinned"});
        let reply = entry("tweet-200", tweet("200", "10", "not-a-time"));
        let context = entry("tweet-201", tweet("201", "99", old));
        let raw=serde_json::to_vec(&json!({"data":{"user":{"result":{"timeline_v2":{"timeline":{"instructions":[
            {"type":"TimelinePinEntry","entry":pinned},
            {"type":"TimelineAddEntries","entries":[{"entryId":"conversation-1","content":{"entryType":"TimelineTimelineModule","items":[{"entryId":"module-200","item":reply["content"]},{"entryId":"module-201","item":context["content"]}]}},
                {"entryId":"unknown-1","content":{"entryType":"FutureTimelineItem"}}]},
            {"type":"TimelineTerminateTimeline","direction":"Bottom"}
        ]}}}}}})).unwrap();
        let result = parse_page("10", &raw, 100)
            .expect("modules and unknown records must retain accounting");
        assert_eq!(result.activities.len(), 2);
        assert!(result.activities[0].pinned);
        assert!(matches!(
            result.activities[1].activity_time,
            TimeEvidence::Unknown { .. }
        ));
        assert!(result.explicit_end);
        assert_eq!(result.dispositions.len(), 2);
        assert!(
            result
                .dispositions
                .iter()
                .any(|d| d.entry_id == "unknown-1")
        );
        assert!(
            parse_page("10", &raw, 2).is_err(),
            "total page budget includes nested items"
        );
    }

    #[test]
    fn free_timeline_repost_variants_do_not_become_original_activity() {
        let date = "Thu Jan 01 00:16:40 +0000 1970";
        let mut newer = tweet("800", "10", date);
        newer["legacy"]["repostedStatusResults"] =
            json!({"result":tweet("50","99","Thu Jan 01 00:01:40 +0000 1970")});
        let mut missing = tweet("801", "10", date);
        missing["legacy"]["retweeted_status_id_str"] = json!("51");
        let result = parse_page(
            "10",
            &page(vec![entry("tweet-800", newer), entry("tweet-801", missing)]),
            100,
        )
        .unwrap();
        assert_eq!(result.activities.len(), 2);
        assert!(
            result
                .activities
                .iter()
                .all(|a| a.kind == ActivityKind::Repost)
        );
        assert_eq!(result.activities[0].source_id, "50");
        assert_eq!(result.activities[1].source_id, "51");
        assert!(matches!(
            result.activities[1].source_time,
            TimeEvidence::Unknown { .. }
        ));
        assert!(matches!(
            result.activities[1].activity_time,
            TimeEvidence::Instant { .. }
        ));
    }
}

#[cfg(test)]
mod scan_tests {
    use super::*;
    fn activity(id: &str, t: Option<i64>, source_time: i64, pinned: bool) -> DiscoveredActivity {
        DiscoveredActivity {
            provider_event_id: Some(id.into()),
            actor_id: Some("10".into()),
            source_id: "20".into(),
            kind: ActivityKind::Repost,
            activity_time: t.map_or(
                TimeEvidence::Unknown {
                    provenance: json!({}),
                },
                |t| TimeEvidence::Instant {
                    timestamp: Timestamp::seconds(t),
                    precision: TimePrecision::Second,
                    provenance: json!({}),
                },
            ),
            source_time: TimeEvidence::Instant {
                timestamp: Timestamp::seconds(source_time),
                precision: TimePrecision::Second,
                provenance: json!({}),
            },
            entry_id: id.into(),
            pinned,
            relations: vec![],
            provenance: json!({}),
        }
    }
    #[test]
    fn scan_uses_activity_time_and_never_stops_at_old_pin_or_out_of_order_entry() {
        let mut scan = Scan::default();
        let page = TimelinePage {
            activities: vec![
                activity("1", Some(1), 1, true),
                activity("2", Some(1000), 1, false),
                activity("3", None, 1000, false),
            ],
            dispositions: vec![],
            next_cursor: Some("next".into()),
            explicit_end: false,
        };
        assert_eq!(scan.observe(&page, 900, 1100).unwrap(), vec![1, 2]);
        assert_eq!(scan.outside_window, 1);
        assert_eq!(scan.uncertain_time, 1);
        assert!(!scan.complete());
        let saved = serde_json::to_vec(&scan).unwrap();
        let mut resumed: Scan = serde_json::from_slice(&saved).unwrap();
        let end = TimelinePage {
            activities: vec![activity("4", Some(1050), 1, false)],
            dispositions: vec![],
            next_cursor: None,
            explicit_end: true,
        };
        assert_eq!(resumed.observe(&end, 900, 1100).unwrap(), vec![0]);
        assert!(resumed.ended);
        assert!(!resumed.complete());
        assert!(resumed.gaps.contains("unknown_activity_time"));
    }
    #[test]
    fn empty_day_requires_explicit_end_and_cursor_cycles_are_not_coverage() {
        let mut scan = Scan::default();
        let mut page = TimelinePage {
            activities: vec![],
            dispositions: vec![],
            next_cursor: None,
            explicit_end: false,
        };
        assert!(scan.observe(&page, 1, 2).is_err());
        assert!(!scan.complete());
        page.explicit_end = true;
        let mut empty = Scan::default();
        empty.observe(&page, 1, 2).unwrap();
        assert!(empty.complete());
        page.explicit_end = false;
        page.next_cursor = Some("same".into());
        let mut repeated = Scan::default();
        repeated.observe(&page, 1, 2).unwrap();
        assert!(repeated.observe(&page, 1, 2).is_err());
        assert!(!repeated.complete());
    }
}

#[cfg(test)]
mod fx_tests {
    use super::*;
    #[test]
    fn anonymous_fx_rows_preserve_unknown_repost_time_and_context() {
        let raw = json!({"code":200,"results":[
            {"id":"100","author":{"id":"10"},"created_timestamp":1700000000,"replying_to":{"status":"99"}},
            {"id":"200","author":{"id":"20"},"created_timestamp":1600000000,"reposted_by":{"id":"10"}},
            {"id":"300","author":{"id":"10"},"created_timestamp":1700000001,"quote":{"id":"299"}},
            {"id":"400","author":{"id":"30"},"created_timestamp":1700000002}
        ],"cursor":{"top":null,"bottom":"next+/="}});
        let page = parse_page("10", &serde_json::to_vec(&raw).unwrap(), 100)
            .expect("anonymous timeline supported");
        assert_eq!(page.activities.len(), 3);
        assert_eq!(page.activities[0].kind, ActivityKind::Reply);
        assert_eq!(
            page.activities[0].relations[0].target,
            Identity::x("99").unwrap()
        );
        assert_eq!(page.activities[1].kind, ActivityKind::Repost);
        assert_eq!(page.activities[1].provider_event_id, None);
        assert!(matches!(
            page.activities[1].activity_time,
            TimeEvidence::Unknown { .. }
        ));
        assert!(matches!(
            page.activities[1].source_time,
            TimeEvidence::Instant { .. }
        ));
        assert_eq!(page.activities[2].kind, ActivityKind::Quote);
        assert_eq!(page.dispositions[0].reason, "context_actor");
        assert_eq!(page.next_cursor.as_deref(), Some("next+/="));
        assert!(!page.explicit_end);
        assert!(
            parse_page(
                "10",
                br#"{"code":500,"results":[],"cursor":{"bottom":null}}"#,
                100
            )
            .is_err()
        );
        assert!(parse_page("10", br#"{"code":200,"results":[]}"#, 100).is_err());
        assert!(
            parse_page(
                "10",
                br#"{"code":200,"results":[],"cursor":{"bottom":null}}"#,
                100
            )
            .unwrap()
            .explicit_end
        );
    }
}
