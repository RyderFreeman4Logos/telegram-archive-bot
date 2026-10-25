use crate::{
    jobs::Failure,
    network::{PublicHttp, Response},
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub struct WebTimeline {
    pub http: PublicHttp,
    pub bearer: String,
    pub cookie: String,
    pub page_size: usize,
    pub max_bytes: u64,
    pub timeout: Duration,
    #[cfg(test)]
    pub(crate) base: &'static str,
}

impl WebTimeline {
    pub async fn lookup(
        &self,
        handle: &str,
        cancel: &CancellationToken,
    ) -> Result<Response, Failure> {
        if handle.is_empty()
            || handle.len() > 15
            || !handle
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(Failure::InvalidInput);
        }
        let variables = serde_json::json!({"screen_name":handle,"withSafetyModeUserFields":true});
        let mut features: serde_json::Value =
            serde_json::from_str(include_str!("timeline_features.json"))
                .map_err(|_| Failure::InvalidInput)?;
        for (key, value) in [
            ("highlights_tweets_tab_ui_enabled", true),
            ("hidden_profile_likes_enabled", true),
            ("creator_subscriptions_tweet_preview_api_enabled", true),
            ("hidden_profile_subscriptions_enabled", true),
            (
                "subscriptions_verification_info_verified_since_enabled",
                true,
            ),
            (
                "subscriptions_verification_info_is_identity_verified_enabled",
                false,
            ),
            ("responsive_web_twitter_article_notes_tab_enabled", false),
            ("subscriptions_feature_can_gift_premium", false),
            (
                "profile_label_improvements_pcf_label_in_post_enabled",
                false,
            ),
        ] {
            features[key] = serde_json::json!(value);
        }
        self.request(
            "AMIBMjtxEEATh4z8V9GtRg/UserByScreenName",
            variables,
            features,
            cancel,
        )
        .await
    }
    pub async fn fetch(
        &self,
        account: &str,
        cursor: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Response, Failure> {
        crate::sources::Identity::x(account).map_err(|_| Failure::InvalidInput)?;
        if cursor.is_some_and(|s| s.is_empty() || s.len() > 16384) {
            return Err(Failure::InvalidInput);
        }
        let mut variables = serde_json::json!({"userId":account,"count":self.page_size,"includePromotedContent":false,"withCommunity":true,"withVoice":true,"withV2Timeline":true});
        if let Some(cursor) = cursor {
            variables["cursor"] = serde_json::json!(cursor);
        }
        let features = serde_json::from_str(include_str!("timeline_features.json"))
            .map_err(|_| Failure::InvalidInput)?;
        self.request(
            "M9Tz6NGTHeRzqHOPdj9I7g/UserTweetsAndReplies",
            variables,
            features,
            cancel,
        )
        .await
    }
    async fn request(
        &self,
        operation: &str,
        variables: serde_json::Value,
        features: serde_json::Value,
        cancel: &CancellationToken,
    ) -> Result<Response, Failure> {
        use reqwest::header::{AUTHORIZATION, COOKIE, HeaderMap, HeaderValue};
        if self.page_size == 0
            || self.page_size > 100
            || self.max_bytes == 0
            || self.timeout.is_zero()
            || self.bearer.is_empty()
            || self.bearer.len() > 16384
            || self.cookie.len() > 16384
        {
            return Err(Failure::InvalidInput);
        }
        let mut cookies = std::collections::BTreeMap::new();
        for part in self.cookie.split(';') {
            let (name, value) = part.trim().split_once('=').ok_or(Failure::InvalidInput)?;
            if name.is_empty() || cookies.insert(name, value).is_some() {
                return Err(Failure::InvalidInput);
            }
        }
        let csrf = cookies
            .get("ct0")
            .filter(|v| !v.is_empty())
            .ok_or(Failure::InvalidInput)?;
        if !cookies.get("auth_token").is_some_and(|v| !v.is_empty()) {
            return Err(Failure::InvalidInput);
        }
        let sensitive = |value: &str| -> Result<HeaderValue, Failure> {
            let mut header = HeaderValue::from_str(value).map_err(|_| Failure::InvalidInput)?;
            header.set_sensitive(true);
            Ok(header)
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            sensitive(&format!("Bearer {}", self.bearer))?,
        );
        headers.insert(COOKIE, sensitive(&self.cookie)?);
        headers.insert("x-csrf-token", sensitive(csrf)?);
        headers.insert(
            "x-twitter-auth-type",
            HeaderValue::from_static("OAuth2Session"),
        );
        headers.insert("x-twitter-active-user", HeaderValue::from_static("yes"));
        headers.insert("x-twitter-client-language", HeaderValue::from_static("en"));
        #[cfg(not(test))]
        let base = "https://x.com";
        #[cfg(test)]
        let base = self.base;
        let mut url = url::Url::parse(&format!("{base}/i/api/graphql/{operation}"))
            .map_err(|_| Failure::InvalidInput)?;
        url.query_pairs_mut()
            .append_pair("variables", &variables.to_string())
            .append_pair("features", &features.to_string());
        self.http
            .request(
                url.as_str(),
                reqwest::Method::GET,
                headers,
                self.max_bytes,
                self.timeout,
                cancel,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn account_timeline_request_preserves_cursor_auth_raw_and_retry_after() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let mut buf = [0u8; 2048];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0 && request.len() + n < 32768);
                request.extend_from_slice(&buf[..n]);
            }
            let body = r#"{"errors":[{"code":88,"message":"Rate limit"}]}"#;
            let response = format!(
                "HTTP/1.1 429 Fixture\r\nContent-Type: application/json\r\nRetry-After: 31\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        let client = WebTimeline {
            http: PublicHttp::fixture_to(address),
            bearer: "PUBLIC_FIXTURE".into(),
            cookie: "auth_token=SESSION_FIXTURE; ct0=CSRF_FIXTURE".into(),
            page_size: 40,
            max_bytes: 10000,
            timeout: Duration::from_secs(2),
            base: "http://fixture.invalid",
        };
        let response = client
            .fetch("123", Some("cursor+/=next"), &CancellationToken::new())
            .await
            .expect("concrete free timeline request");
        assert_eq!(response.status, 429);
        assert_eq!(response.retry_after_seconds, Some(31));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.bytes).unwrap()["errors"][0]["code"],
            88
        );
        let request = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        let request_lower = request.to_ascii_lowercase();
        assert!(request_lower.contains("authorization: bearer public_fixture\r\n"));
        assert!(request_lower.contains("x-csrf-token: csrf_fixture\r\n"));
        assert!(request_lower.contains("cookie: auth_token=session_fixture; ct0=csrf_fixture\r\n"));
        let path = request
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let url = url::Url::parse(&format!("http://fixture.invalid{path}")).unwrap();
        assert!(url.path().ends_with("/UserTweetsAndReplies"));
        let query = url
            .query_pairs()
            .collect::<std::collections::BTreeMap<_, _>>();
        let vars: serde_json::Value = serde_json::from_str(&query["variables"]).unwrap();
        assert_eq!(vars["cursor"], "cursor+/=next");
        assert_eq!(vars["userId"], "123");
        assert!(!path.contains("SESSION_FIXTURE"));
    }
}

#[cfg(test)]
mod lookup_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn handle_lookup_is_authenticated_and_returns_strong_id_raw_evidence() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut raw = Vec::new();
            while !raw.windows(4).any(|x| x == b"\r\n\r\n") {
                let mut b = [0; 2048];
                let n = socket.read(&mut b).await.unwrap();
                assert!(n > 0 && raw.len() + n < 32768);
                raw.extend_from_slice(&b[..n]);
            }
            let body = r#"{"data":{"user":{"result":{"rest_id":"123","core":{"screen_name":"Fixture"}}}}}"#;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            String::from_utf8(raw).unwrap()
        });
        let client = WebTimeline {
            http: PublicHttp::fixture_to(addr),
            bearer: "PUBLIC_FIXTURE".into(),
            cookie: "auth_token=SESSION_FIXTURE; ct0=CSRF_FIXTURE".into(),
            page_size: 40,
            max_bytes: 10000,
            timeout: Duration::from_secs(2),
            base: "http://fixture.invalid",
        };
        let response = client
            .lookup("Fixture", &CancellationToken::new())
            .await
            .expect("real handle resolution request");
        assert_eq!(response.status, 200);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response.bytes)
                .unwrap()
                .pointer("/data/user/result/rest_id")
                .unwrap(),
            "123"
        );
        let raw = server.await.unwrap();
        let path = raw
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap();
        let url = url::Url::parse(&format!("http://fixture.invalid{path}")).unwrap();
        assert!(url.path().ends_with("/UserByScreenName"));
        let query = url
            .query_pairs()
            .collect::<std::collections::BTreeMap<_, _>>();
        let vars: serde_json::Value = serde_json::from_str(&query["variables"]).unwrap();
        assert_eq!(vars["screen_name"], "Fixture");
        assert!(
            raw.to_ascii_lowercase()
                .contains("x-csrf-token: csrf_fixture\r\n")
        );
        assert!(
            client
                .lookup("bad/handle", &CancellationToken::new())
                .await
                .is_err()
        );
    }
}
