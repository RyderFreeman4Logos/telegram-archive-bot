use super::{Post, Provider, parse_fx};
use crate::{jobs::Failure, network::PublicHttp, sources::FetchStatus};
use futures_util::{FutureExt, future::BoxFuture};
use reqwest::{
    Method,
    header::{HeaderMap, HeaderValue},
};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

const QUERY: &str = "LkId5Akr61BS6BmOIcffRg";
const FEATURES: &str = include_str!("x_guest_features.json");

pub(super) struct Guest {
    http: PublicHttp,
    bearer: String,
    timeout: Duration,
    token: Mutex<Option<(String, Instant)>>,
    base: &'static str,
}

impl Guest {
    pub(super) fn new(http: PublicHttp, bearer: String, timeout: Duration) -> Self {
        Self {
            http,
            bearer,
            timeout,
            token: Mutex::new(None),
            base: "https://api.x.com",
        }
    }

    fn headers(&self, token: Option<&str>) -> Result<HeaderMap, Failure> {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("authorization", format!("Bearer {}",self.bearer)),
            ("origin", "https://x.com".into()),
            ("referer", "https://x.com/".into()),
            ("x-twitter-active-user", "yes".into()),
            ("x-twitter-client-language", "en".into()),
            ("user-agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36".into()),
        ] {
            let mut value = HeaderValue::from_str(&value).map_err(|_| Failure::InvalidInput)?;
            value.set_sensitive(name == "authorization");
            headers.insert(name,value);
        }
        if let Some(token) = token {
            let mut value = HeaderValue::from_str(token).map_err(|_| Failure::InvalidOutput)?;
            value.set_sensitive(true);
            headers.insert("x-guest-token", value);
        }
        Ok(headers)
    }

    async fn fetch_inner(
        &self,
        id: &str,
        max_bytes: u64,
        cancel: &CancellationToken,
    ) -> Result<Post, Failure> {
        let mut remaining = max_bytes;
        let mut rejected_token_status = None;
        let mut rejected_token = None;
        for refresh in 0..2 {
            let (token, reused) = {
                let mut cached = tokio::select! { biased;
                    _=cancel.cancelled()=>return Err(Failure::Cancelled), value=self.token.lock()=>value,
                };
                let reused = cached
                    .as_ref()
                    .is_some_and(|(_, time)| time.elapsed() < Duration::from_secs(3600));
                if !reused {
                    *cached = None;
                    let response = self
                        .http
                        .request(
                            &format!("{}/1.1/guest/activate.json", self.base),
                            Method::POST,
                            self.headers(None)?,
                            remaining.min(16384),
                            self.timeout,
                            cancel,
                        )
                        .await?;
                    remaining = remaining.saturating_sub(response.bytes.len() as u64);
                    if response.status != 200 {
                        let status = if matches!(response.status, 401 | 403) {
                            FetchStatus::Forbidden
                        } else {
                            FetchStatus::Error
                        };
                        let mut post =
                            Post::unavailable(id, status, "guest_activation_unavailable");
                        post.evidence["http_status"] = json!(response.status);
                        post.evidence["operation"] = json!("guest_activation");
                        return Ok(post);
                    }
                    let value: Value = serde_json::from_slice(&response.bytes)
                        .map_err(|_| Failure::InvalidOutput)?;
                    let token = value["guest_token"]
                        .as_str()
                        .filter(|t| !t.is_empty() && t.len() <= 512)
                        .ok_or(Failure::InvalidOutput)?;
                    *cached = Some((token.into(), Instant::now()));
                }
                (cached.as_ref().ok_or(Failure::Internal)?.0.clone(), reused)
            };
            let mut url = url::Url::parse(&format!(
                "{}/graphql/{QUERY}/TweetResultByRestId",
                self.base
            ))
            .map_err(|_| Failure::Internal)?;
            url.query_pairs_mut()
                .append_pair("variables", &json!({"tweetId":id,"withCommunity":false,"includePromotedContent":false,"withVoice":false}).to_string())
                .append_pair("features",FEATURES)
                .append_pair("fieldToggles", &json!({"withArticlePlainText":true,"withArticleRichContentState":true}).to_string());
            let response = self
                .http
                .request(
                    url.as_str(),
                    Method::GET,
                    self.headers(Some(&token))?,
                    remaining,
                    self.timeout,
                    cancel,
                )
                .await?;
            remaining = remaining.saturating_sub(response.bytes.len() as u64);
            if matches!(response.status, 401 | 403) && refresh == 0 {
                rejected_token_status = Some(response.status);
                rejected_token = Some(token.clone());
                let mut cached = tokio::select! { biased;
                    _=cancel.cancelled()=>return Err(Failure::Cancelled), value=self.token.lock()=>value,
                };
                if cached.as_ref().is_some_and(|(value, _)| value == &token) {
                    *cached = None;
                }
                continue;
            }
            let payload: Result<Value, _> = serde_json::from_slice(&response.bytes);
            let secrets = [
                token.as_str(),
                self.bearer.as_str(),
                rejected_token.as_deref().unwrap_or_default(),
            ];
            // JSON escapes must not hide authentication material in decoded content or raw-only fields.
            let echoes_auth = secrets.iter().filter(|s| !s.is_empty()).any(|secret| {
                response
                    .bytes
                    .windows(secret.len())
                    .any(|part| part == secret.as_bytes())
            }) || payload
                .as_ref()
                .is_ok_and(|value| contains_auth(value, &secrets));
            let mut post = match response.status {
                200 => payload
                    .map_err(|_| Failure::InvalidOutput)
                    .and_then(|value| parse(id, &value))
                    .unwrap_or_else(|_| {
                        Post::unavailable(id, FetchStatus::Error, "guest_invalid_payload")
                    }),
                401 | 403 => {
                    Post::unavailable(id, FetchStatus::Forbidden, "guest_article_access_denied")
                }
                404 => Post::unavailable(id, FetchStatus::Missing, "guest_article_not_found"),
                429 => Post::unavailable(id, FetchStatus::Error, "guest_article_rate_limited"),
                _ => Post::unavailable(id, FetchStatus::Error, "guest_article_endpoint_error"),
            };
            // Activation payloads and accidentally echoed authentication material are never archived.
            if echoes_auth {
                post = Post::unavailable(
                    id,
                    FetchStatus::Error,
                    "guest_response_contains_auth_material",
                );
                post.evidence["raw_response_discarded_for_credentials"] = json!(true);
            } else {
                post.raw = response.bytes;
            }
            post.evidence["http_status"] = json!(response.status);
            post.evidence["operation"] = json!("article_query");
            post.evidence["query_id"] = json!(QUERY);
            post.evidence["guest_token_reused"] = json!(reused);
            post.evidence["guest_refreshes"] = json!(refresh);
            post.evidence["rejected_token_http_status"] = json!(rejected_token_status);
            post.evidence["network_body_bytes"] = json!(max_bytes - remaining);
            return Ok(post);
        }
        Err(Failure::Internal)
    }
}

fn contains_auth(value: &Value, secrets: &[&str]) -> bool {
    let contains = |text: &str| {
        secrets
            .iter()
            .any(|secret| !secret.is_empty() && text.contains(secret))
    };
    match value {
        Value::String(text) => contains(text),
        Value::Array(values) => values.iter().any(|value| contains_auth(value, secrets)),
        Value::Object(fields) => fields
            .iter()
            .any(|(key, value)| contains(key) || contains_auth(value, secrets)),
        _ => false,
    }
}

impl Provider for Guest {
    fn name(&self) -> &'static str {
        "x-guest-article-v1"
    }
    fn article_only(&self) -> bool {
        true
    }
    fn fetch<'a>(
        &'a self,
        id: &'a str,
        max_bytes: u64,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Post, Failure>> {
        async move {
            tokio::select! { biased;
                _=cancel.cancelled()=>Err(Failure::Cancelled),
                result=tokio::time::timeout(self.timeout,self.fetch_inner(id,max_bytes,cancel))=>result.map_err(|_| Failure::Timeout)?,
            }
        }.boxed()
    }
}

fn parse(id: &str, payload: &Value) -> Result<Post, Failure> {
    let result = &payload["data"]["tweetResult"]["result"];
    let result = result.get("tweet").unwrap_or(result);
    if result.is_null() || result["__typename"] == "TweetUnavailable" {
        return Ok(Post::unavailable(
            id,
            FetchStatus::Missing,
            "guest_returned_no_post",
        ));
    }
    if result["rest_id"] != id {
        return Err(Failure::InvalidOutput);
    }
    let article = &result["article"]["article_results"]["result"];
    let mut blocks = article["content_state"]["blocks"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    if !blocks
        .iter()
        .any(|b| b["type"] != "atomic" && b["text"].as_str().is_some_and(|t| !t.trim().is_empty()))
        && let Some(plain) = article["plain_text"]
            .as_str()
            .or_else(|| article["plainText"].as_str())
            .filter(|s| !s.trim().is_empty())
    {
        blocks.push(json!({"type":"unstyled","text":plain}));
    }
    let user = &result["core"]["user_results"]["result"];
    let legacy = &result["legacy"];
    let mut post = parse_fx(
        id,
        &json!({"tweet":{
            "id":id,"text":legacy["full_text"],"lang":legacy["lang"],"created_at":legacy["created_at"],
            "author":{"name":user["core"].get("name").or_else(||user["legacy"].get("name"))},
            "replying_to_status":legacy["in_reply_to_status_id_str"],
            "quote":legacy["quoted_status_id_str"].as_str().map(|id|json!({"id":id})),
            "article":{"title":article["title"],"preview_text":article["preview_text"],"content":{"blocks":blocks,"entityMap":article["content_state"]["entityMap"]}}
        }}),
    )?;
    post.evidence["article_present"] = json!(!article.is_null());
    Ok(post)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn payload(article: Value) -> Value {
        json!({"data":{"tweetResult":{"result":{"tweet":{
            "rest_id":"100","legacy":{"full_text":"Article preview","lang":"en","in_reply_to_status_id_str":"200","quoted_status_id_str":"300"},
            "core":{"user_results":{"result":{"core":{"name":"Fixture author"}}}},
            "article":{"article_results":{"result":article}}
        }}}}})
    }

    #[test]
    fn nested_plain_rich_and_missing_bodies_keep_identity_and_completeness() {
        for article in [
            json!({"title":"Title","plain_text":"Complete article body"}),
            json!({"title":"Title","plainText":"Complete article body"}),
            json!({"title":"Title","content_state":{"blocks":[{"type":"header-two","text":"Complete article body"}]}}),
        ] {
            let post = parse("100", &payload(article)).unwrap();
            assert_eq!(post.status, FetchStatus::Complete);
            assert_eq!(post.evidence["article_hydrated"], true);
            assert!(
                post.content
                    .as_ref()
                    .unwrap()
                    .body
                    .contains("Complete article body")
            );
            assert_eq!(
                post.content.unwrap().metadata.author.as_deref(),
                Some("Fixture author")
            );
            assert_eq!(post.relations.len(), 2);
        }
        for article in [
            Value::Null,
            json!({}),
            json!({"content_state":{"blocks":[]}}),
        ] {
            let post = parse("100", &payload(article)).unwrap();
            assert_eq!(post.status, FetchStatus::Partial);
            assert_eq!(post.evidence["article_hydrated"], false);
        }
        let media = parse("100",&payload(json!({"plain_text":"Complete article body","content_state":{"blocks":[{"type":"atomic"}]}}))).unwrap();
        assert_eq!(media.status, FetchStatus::Partial);
        assert_eq!(media.evidence["article_hydrated"], true);
        assert!(matches!(
            parse("999", &payload(json!({"plain_text":"Wrong identity"}))),
            Err(Failure::InvalidOutput)
        ));
        assert_eq!(
            parse("100", &json!({})).unwrap().status,
            FetchStatus::Missing
        );
    }

    struct Fixture {
        guest: Guest,
        requests: Arc<StdMutex<Vec<String>>>,
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.task.abort();
        }
    }
    impl Fixture {
        async fn new(replies: Vec<(u16, Value)>) -> Self {
            Self::raw(
                replies
                    .into_iter()
                    .map(|(status, body)| (status, body.to_string()))
                    .collect(),
            )
            .await
        }
        async fn raw(replies: Vec<(u16, String)>) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut guest = Guest::new(
                PublicHttp::fixture_to(listener.local_addr().unwrap()),
                "SYNTHETIC_PUBLIC_CLIENT".into(),
                Duration::from_secs(2),
            );
            guest.base = "http://fixture.invalid";
            let requests = Arc::new(StdMutex::new(Vec::new()));
            let saved = requests.clone();
            let task = tokio::spawn(async move {
                for (status, body) in replies {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut input = Vec::new();
                    while !input.windows(4).any(|s| s == b"\r\n\r\n") {
                        let mut buffer = [0u8; 2048];
                        let read = socket.read(&mut buffer).await.unwrap();
                        assert!(read > 0 && input.len() + read <= 16384);
                        input.extend_from_slice(&buffer[..read]);
                    }
                    saved
                        .lock()
                        .unwrap()
                        .push(String::from_utf8(input).unwrap());
                    let response = format!(
                        "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                }
            });
            Self {
                guest,
                requests,
                task,
            }
        }
    }

    #[tokio::test]
    async fn refreshes_once_reuses_token_and_retains_only_query_raw() {
        let body = payload(json!({"plain_text":"Complete article body"}));
        let fixture = Fixture::new(vec![
            (200, json!({"guest_token":"SYNTHETIC_GUEST_ONE"})),
            (401, json!({"error":"expired"})),
            (200, json!({"guest_token":"SYNTHETIC_GUEST_TWO"})),
            (200, body.clone()),
            (200, body.clone()),
            (200, json!({"guest_token":"SYNTHETIC_GUEST_THREE"})),
            (200, body.clone()),
        ])
        .await;
        let first = fixture
            .guest
            .fetch("100", 32768, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(first.raw, body.to_string().as_bytes());
        assert_eq!(first.evidence["guest_refreshes"], 1);
        assert_eq!(first.evidence["rejected_token_http_status"], 401);
        let second = fixture
            .guest
            .fetch("100", 32768, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(second.evidence["guest_token_reused"], true);
        fixture.guest.token.lock().await.as_mut().unwrap().1 =
            Instant::now() - Duration::from_secs(3601);
        let expired = fixture
            .guest
            .fetch("100", 32768, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(expired.evidence["guest_token_reused"], false);
        let requests = fixture.requests.lock().unwrap();
        assert_eq!(requests.len(), 7);
        assert!(requests[0].starts_with("POST /1.1/guest/activate.json "));
        assert!(!requests[0].contains("x-guest-token"));
        assert!(requests[1].contains("x-guest-token: SYNTHETIC_GUEST_ONE"));
        assert!(requests[3].contains("x-guest-token: SYNTHETIC_GUEST_TWO"));
        let target = requests[3].split_whitespace().nth(1).unwrap();
        let url = url::Url::parse(&format!("http://fixture.invalid{target}")).unwrap();
        let variables: Value =
            serde_json::from_str(&url.query_pairs().find(|(k, _)| k == "variables").unwrap().1)
                .unwrap();
        assert_eq!(variables["tweetId"], "100");
        assert!(
            !serde_json::to_string(&first)
                .unwrap()
                .contains("SYNTHETIC_GUEST")
        );
    }

    #[tokio::test]
    async fn denial_errors_and_echoed_credentials_never_become_content() {
        let fixture = Fixture::new(vec![(
            403,
            json!({"guest_token":"MUST_NOT_RETAIN_ACTIVATION"}),
        )])
        .await;
        let post = fixture
            .guest
            .fetch("100", 32768, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(post.status, FetchStatus::Forbidden);
        assert!(post.raw.is_empty() && post.content.is_none());
        for status in [403, 404, 429, 500] {
            let mut replies = vec![(200, json!({"guest_token":"SYNTHETIC_GUEST_ONE"}))];
            if status == 403 {
                replies.extend([
                    (403, json!({"denied":true})),
                    (200, json!({"guest_token":"SYNTHETIC_GUEST_TWO"})),
                ]);
            }
            replies.push((status, json!({"denied":true})));
            let fixture = Fixture::new(replies).await;
            let post = fixture
                .guest
                .fetch("100", 32768, &CancellationToken::new())
                .await
                .unwrap();
            assert!(post.content.is_none());
            assert_eq!(post.evidence["http_status"], status);
            assert_eq!(post.raw, b"{\"denied\":true}");
            assert_eq!(
                fixture.requests.lock().unwrap().len(),
                if status == 403 { 4 } else { 2 }
            );
        }
        let fixture = Fixture::new(vec![
            (200, json!({"guest_token":"SYNTHETIC_GUEST_ONE"})),
            (200, json!({"echo":"SYNTHETIC_GUEST_ONE"})),
        ])
        .await;
        let post = fixture
            .guest
            .fetch("100", 32768, &CancellationToken::new())
            .await
            .unwrap();
        assert!(post.raw.is_empty() && post.content.is_none());
        assert_eq!(
            post.evidence["raw_response_discarded_for_credentials"],
            true
        );
    }

    #[tokio::test]
    async fn escaped_authentication_echoes_cannot_enter_canonical_storage() {
        use std::os::unix::fs::PermissionsExt;
        for secret in ["SYNTHETIC_GUEST_ONE", "SYNTHETIC_PUBLIC_CLIENT"] {
            for location in ["body", "key", "nested_value"] {
                for refresh in [false, true] {
                    let mut response = payload(
                        json!({"plain_text":if location == "body" {secret} else {"Benign body"}}),
                    );
                    if location == "key" {
                        response[secret] = json!(true);
                    }
                    if location == "nested_value" {
                        response["extra"] = json!({"nested":[secret]});
                    }
                    let wire = response
                        .to_string()
                        .replace("SYNTHETIC_", r"\u0053YNTHETIC_");
                    assert!(!wire.contains(secret));
                    let mut replies = vec![(
                        200,
                        json!({"guest_token":"SYNTHETIC_GUEST_ONE"}).to_string(),
                    )];
                    if refresh {
                        replies.extend([
                            (401, json!({"denied":true}).to_string()),
                            (
                                200,
                                json!({"guest_token":"SYNTHETIC_GUEST_TWO"}).to_string(),
                            ),
                        ]);
                    }
                    replies.push((200, wire));
                    let fixture = Fixture::raw(replies).await;
                    let post = fixture
                        .guest
                        .fetch("100", 32768, &CancellationToken::new())
                        .await
                        .unwrap();
                    assert!(
                        post.content.is_none() && post.raw.is_empty(),
                        "decoded credentials must not become content or raw evidence"
                    );
                    assert_eq!(
                        post.evidence["raw_response_discarded_for_credentials"],
                        true
                    );
                    let directory = tempfile::Builder::new()
                        .permissions(std::fs::Permissions::from_mode(0o700))
                        .tempdir()
                        .unwrap();
                    let store = crate::storage::Store::open(directory.path()).unwrap();
                    let capture = crate::sources::Capture {
                        identity: crate::sources::Identity::x("100").unwrap(),
                        submitted_url: None,
                        content: post.content,
                        status: post.status,
                        extractor: fixture.guest.name().into(),
                        evidence: post.evidence,
                        raw_digest: None,
                        relations: post.relations,
                    };
                    let receipt = store.capture(&capture).unwrap();
                    assert!(receipt.is_none());
                    store.integrity().unwrap();
                    assert_eq!(
                        std::fs::read_dir(directory.path().join("blobs"))
                            .unwrap()
                            .count(),
                        0
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn guest_budgets_cover_activation_lock_body_and_cancellation() {
        let fixture = Fixture::new(vec![(200, json!({"guest_token":"X".repeat(20000)}))]).await;
        assert!(matches!(
            fixture
                .guest
                .fetch("100", 32768, &CancellationToken::new())
                .await,
            Err(Failure::InvalidOutput)
        ));
        let mut fixture = Fixture::new(vec![]).await;
        fixture.guest.timeout = Duration::from_millis(20);
        let guard = fixture.guest.token.lock().await;
        assert!(matches!(
            fixture
                .guest
                .fetch("100", 32768, &CancellationToken::new())
                .await,
            Err(Failure::Timeout)
        ));
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            fixture.guest.fetch("100", 32768, &cancel).await,
            Err(Failure::Cancelled)
        ));
        drop(guard);
        assert!(fixture.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    #[ignore = "explicit real guest endpoint probe; requires private bearer-file and post environment"]
    async fn actual_guest_article_probe() -> anyhow::Result<()> {
        let id = std::env::var("ARCHIVE_GUEST_LIVE_POST")?;
        let bearer =
            crate::config::SecretRef::File(std::env::var("ARCHIVE_GUEST_BEARER_FILE")?.into())
                .read()?;
        let guest = Guest::new(PublicHttp::default(), bearer, Duration::from_secs(30));
        let post = guest
            .fetch(&id, 4_000_000, &CancellationToken::new())
            .await?;
        let state = std::env::var("ARCHIVE_GUEST_EVIDENCE_DIR")?;
        let store = crate::storage::Store::open(std::path::Path::new(&state))?;
        let blob = store.publish_bytes(&post.raw)?;
        let revision = store.capture(&crate::sources::Capture {
            identity: crate::sources::Identity::x(&id)?,
            submitted_url: None,
            content: post.content.clone(),
            status: post.status,
            extractor: guest.name().into(),
            evidence: post.evidence.clone(),
            raw_digest: Some(blob.digest),
            relations: post.relations.clone(),
        })?;
        store.integrity()?;
        let payload: Value = serde_json::from_slice(&post.raw)?;
        let tweet = &payload["data"]["tweetResult"]["result"];
        let tweet = tweet.get("tweet").unwrap_or(tweet);
        let blocks = tweet["article"]["article_results"]["result"]["content_state"]["blocks"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let expected = blocks
            .iter()
            .filter(|b| b["type"] != "atomic")
            .filter_map(|b| b["text"].as_str())
            .filter(|t| !t.trim().is_empty())
            .collect::<Vec<_>>();
        let matched = expected
            .iter()
            .filter(|t| post.content.as_ref().is_some_and(|c| c.body.contains(**t)))
            .count();
        anyhow::ensure!(
            !expected.is_empty() && matched == expected.len() && revision.is_some(),
            "real textual blocks or canonical revision missing"
        );
        println!(
            "{}",
            json!({"canonical_integrity_passed":true,"expected_text_blocks":expected.len(),"retained_text_blocks":matched})
        );
        println!(
            "{}",
            json!({"status":post.status,"article_hydrated":post.evidence["article_hydrated"],"body_bytes":post.content.as_ref().map(|c|c.body.len()),"raw_digest":crate::digest(&post.raw),"raw_bytes":post.raw.len(),"evidence":post.evidence})
        );
        anyhow::ensure!(
            post.evidence["article_hydrated"] == true && !post.raw.is_empty(),
            "live guest article body unavailable; see sanitized observation"
        );
        Ok(())
    }
}
