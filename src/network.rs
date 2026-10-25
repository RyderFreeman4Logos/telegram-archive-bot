use crate::jobs::Failure;
use futures_util::StreamExt;
use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use url::Url;

pub fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && (b == 168 || (b == 0 && c <= 2) || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            if let Some(ip) = ip.to_ipv4_mapped() {
                return public_ip(IpAddr::V4(ip));
            }
            let s = ip.segments();
            s[0] & 0xe000 == 0x2000
                && !(s[0] == 0x2001 && (s[1] <= 0x01ff || s[1] == 0x0db8))
                && s[0] != 0x2002
        }
    }
}

pub fn source_url(input: &str) -> Result<Url, Failure> {
    let url = Url::parse(input).map_err(|_| Failure::InvalidInput)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(Failure::InvalidInput);
    }
    if let Ok(ip) = url
        .host_str()
        .unwrap()
        .trim_matches(['[', ']'])
        .parse::<IpAddr>()
        && !public_ip(ip)
    {
        return Err(Failure::InvalidInput);
    }
    Ok(url)
}

pub struct Response {
    pub submitted: Url,
    pub resolved: Url,
    pub redirects: Vec<Url>,
    pub status: u16,
    pub content_type: String,
    pub charset: Option<String>,
    pub bytes: Vec<u8>,
    pub retry_after_seconds: Option<u64>,
}

#[derive(Clone, Default)]
pub struct PublicHttp {
    #[cfg(test)]
    fixture: Option<FixtureRoute>,
}
#[cfg(test)]
#[derive(Clone)]
struct FixtureRoute {
    addresses: Vec<SocketAddr>,
    connect: SocketAddr,
}
impl PublicHttp {
    #[cfg(test)]
    pub(crate) fn fixture_to(connect: SocketAddr) -> Self {
        Self {
            fixture: Some(FixtureRoute {
                addresses: vec!["93.184.216.34:80".parse().unwrap()],
                connect,
            }),
        }
    }

    pub async fn get(
        &self,
        input: &str,
        max_bytes: u64,
        timeout: Duration,
        bearer: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Response, Failure> {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(bearer) = bearer {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {bearer}"))
                .map_err(|_| Failure::InvalidInput)?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        self.request(
            input,
            reqwest::Method::GET,
            headers,
            max_bytes,
            timeout,
            cancel,
        )
        .await
    }

    pub(crate) async fn request(
        &self,
        input: &str,
        method: reqwest::Method,
        headers: reqwest::header::HeaderMap,
        max_bytes: u64,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<Response, Failure> {
        let request = self.request_inner(input, method, headers, max_bytes);
        tokio::select! {biased;
            _=cancel.cancelled()=>Err(Failure::Cancelled),
            result=tokio::time::timeout(timeout,request)=>result.map_err(|_|Failure::Timeout)?,
        }
    }

    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, Failure> {
        #[cfg(test)]
        if let Some(route) = &self.fixture {
            return Ok(route.addresses.clone());
        }
        Ok(tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| Failure::Network)?
            .collect())
    }

    async fn request_inner(
        &self,
        input: &str,
        method: reqwest::Method,
        headers: reqwest::header::HeaderMap,
        max_bytes: u64,
    ) -> Result<Response, Failure> {
        let submitted = source_url(input)?;
        let mut url = submitted.clone();
        let mut redirects = vec![];
        loop {
            if !headers.is_empty() && url.origin() != submitted.origin() {
                return Err(Failure::InvalidInput);
            }
            let host = url
                .host_str()
                .ok_or(Failure::InvalidInput)?
                .trim_matches(['[', ']']);
            let port = url.port_or_known_default().ok_or(Failure::InvalidInput)?;
            let addresses = self.resolve(host, port).await?;
            if addresses.is_empty() || addresses.iter().any(|a| !public_ip(a.ip())) {
                return Err(Failure::InvalidInput);
            }
            #[cfg(test)]
            let addresses = if let Some(route) = &self.fixture {
                vec![route.connect]
            } else {
                addresses
            };
            // Pin the checked addresses for this hop; the HTTP client cannot re-resolve them.
            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .resolve_to_addrs(host, &addresses)
                .user_agent(concat!("telegram-archive-bot/", env!("CARGO_PKG_VERSION")))
                .build()
                .map_err(|_| Failure::Internal)?;
            let request = client
                .request(method.clone(), url.clone())
                .headers(headers.clone());
            let response = request.send().await.map_err(|e| {
                if e.is_timeout() {
                    Failure::Timeout
                } else {
                    Failure::Network
                }
            })?;
            let status = response.status();
            if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
                if method != reqwest::Method::GET || redirects.len() >= 5 {
                    return Err(Failure::InvalidInput);
                }
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|h| h.to_str().ok())
                    .ok_or(Failure::InvalidOutput)?;
                let next = url.join(location).map_err(|_| Failure::InvalidOutput)?;
                url = source_url(next.as_str())?;
                redirects.push(url.clone());
                continue;
            }
            if response
                .content_length()
                .is_some_and(|length| length > max_bytes)
            {
                return Err(Failure::InvalidOutput);
            }
            let content_header = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok())
                .unwrap_or("");
            let charset = content_header.split(';').skip(1).find_map(|part| {
                let (key, value) = part.trim().split_once('=')?;
                key.eq_ignore_ascii_case("charset")
                    .then(|| value.trim().trim_matches(['\'', '"']).to_owned())
            });
            let content_type = content_header
                .split(';')
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            let retry_after_seconds = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok());
            let mut stream = response.bytes_stream();
            let mut bytes = vec![];
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| Failure::Network)?;
                if (bytes.len() as u64).saturating_add(chunk.len() as u64) > max_bytes {
                    return Err(Failure::InvalidOutput);
                }
                bytes.extend_from_slice(&chunk);
            }
            return Ok(Response {
                submitted,
                resolved: url,
                redirects,
                status: status.as_u16(),
                content_type,
                charset,
                bytes,
                retry_after_seconds,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn network_boundaries_block_private_metadata_and_special_addresses() {
        for ip in [
            "0.0.0.0",
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.1.1",
            "198.18.0.1",
            "224.0.0.1",
            "::1",
            "fc00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
            "2001:db8::1",
            "2002:7f00:1::1",
        ] {
            assert!(!public_ip(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            assert!(public_ip(ip.parse().unwrap()));
        }
        for url in [
            "file:///etc/passwd",
            "http://127.1/",
            "http://2130706433/",
            "http://[::1]/",
            "http://u:p@example.com/",
        ] {
            assert!(source_url(url).is_err(), "{url}");
        }
        assert!(source_url("https://example.org/?s=one#/route").is_ok());
    }
    async fn fixture(reply: &'static [u8]) -> (PublicHttp, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connect = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            let _ = socket.read(&mut buf).await;
            socket.write_all(reply).await.unwrap();
        });
        (
            PublicHttp {
                fixture: Some(FixtureRoute {
                    addresses: vec!["1.1.1.1:80".parse().unwrap()],
                    connect,
                }),
            },
            server,
        )
    }

    #[tokio::test]
    async fn redirects_cannot_reach_private_addresses_or_forward_auth() {
        let cancel = CancellationToken::new();
        for (reply, bearer) in [
            (b"HTTP/1.1 302 Found\r\nLocation: http://169.254.169.254/latest/\r\nContent-Length: 0\r\n\r\n".as_slice(), None),
            (b"HTTP/1.1 302 Found\r\nLocation: http://another.invalid/\r\nContent-Length: 0\r\n\r\n".as_slice(), Some("synthetic-secret")),
        ] {
            let (http, server) = fixture(reply).await;
            assert!(matches!(http.get("http://fixture.invalid/start", 100, Duration::from_secs(2), bearer, &cancel).await, Err(Failure::InvalidInput)));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn guest_headers_cannot_cross_origins_and_activation_never_redirects() {
        for (method, location) in [
            (reqwest::Method::GET, "http://another.invalid/query"),
            (reqwest::Method::POST, "http://fixture.invalid/other"),
        ] {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let http = PublicHttp::fixture_to(listener.local_addr().unwrap());
            let expected = method.to_string();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = [0; 4096];
                let length = socket.read(&mut buffer).await.unwrap();
                let request = String::from_utf8_lossy(&buffer[..length]);
                assert!(request.starts_with(&expected));
                assert!(request.contains("x-guest-token: synthetic-guest"));
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            });
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                "x-guest-token",
                reqwest::header::HeaderValue::from_static("synthetic-guest"),
            );
            assert!(matches!(
                http.request(
                    "http://fixture.invalid/query",
                    method,
                    headers,
                    100,
                    Duration::from_secs(2),
                    &CancellationToken::new()
                )
                .await,
                Err(Failure::InvalidInput)
            ));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn stream_and_declared_sizes_are_bounded_and_charset_retained() {
        let cancel = CancellationToken::new();
        for reply in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 10000\r\n\r\n".as_slice(),
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n6\r\n123456\r\n0\r\n\r\n"
                .as_slice(),
        ] {
            let (http, server) = fixture(reply).await;
            assert!(matches!(
                http.get(
                    "http://fixture.invalid/file",
                    5,
                    Duration::from_secs(2),
                    None,
                    &cancel
                )
                .await,
                Err(Failure::InvalidOutput)
            ));
            server.await.unwrap();
        }
        let (http, server) = fixture(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=\"UTF-8\"\r\nContent-Length: 3\r\n\r\nabc").await;
        let response = http
            .get(
                "http://fixture.invalid/file",
                5,
                Duration::from_secs(2),
                None,
                &cancel,
            )
            .await
            .unwrap();
        assert_eq!(response.bytes, b"abc");
        assert_eq!(response.charset.as_deref(), Some("UTF-8"));
        server.await.unwrap();
        let mut mixed = http;
        mixed
            .fixture
            .as_mut()
            .unwrap()
            .addresses
            .push("10.0.0.1:80".parse().unwrap());
        assert!(matches!(
            mixed
                .get(
                    "http://fixture.invalid/file",
                    5,
                    Duration::from_secs(2),
                    None,
                    &cancel
                )
                .await,
            Err(Failure::InvalidInput)
        ));
    }
    #[tokio::test]
    async fn deadline_and_cancellation_cover_a_silent_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for cancelled in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let connect = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = [0; 4096];
                let _ = socket.read(&mut buf).await;
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\n")
                    .await
                    .unwrap();
                let mut byte = [0];
                assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
            });
            let http = PublicHttp {
                fixture: Some(FixtureRoute {
                    addresses: vec!["1.1.1.1:80".parse().unwrap()],
                    connect,
                }),
            };
            let token = CancellationToken::new();
            if cancelled {
                let copy = token.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    copy.cancel();
                });
            }
            let result = http
                .get(
                    "http://fixture.invalid/slow",
                    10,
                    Duration::from_millis(100),
                    None,
                    &token,
                )
                .await;
            assert!(
                matches!(result, Err(Failure::Cancelled)) && cancelled
                    || matches!(result, Err(Failure::Timeout)) && !cancelled
            );
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap();
        }
    }
}
