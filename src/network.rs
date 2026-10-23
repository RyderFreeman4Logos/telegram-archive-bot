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
    pub bytes: Vec<u8>,
}

#[derive(Clone, Default)]
pub struct PublicHttp;
impl PublicHttp {
    pub async fn get(
        &self,
        input: &str,
        max_bytes: u64,
        timeout: Duration,
        bearer: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Response, Failure> {
        let request = self.get_inner(input, max_bytes, bearer);
        tokio::select! {biased;
            _=cancel.cancelled()=>Err(Failure::Cancelled),
            result=tokio::time::timeout(timeout,request)=>result.map_err(|_|Failure::Timeout)?,
        }
    }

    async fn get_inner(
        &self,
        input: &str,
        max_bytes: u64,
        bearer: Option<&str>,
    ) -> Result<Response, Failure> {
        let submitted = source_url(input)?;
        let mut url = submitted.clone();
        let mut redirects = vec![];
        loop {
            let host = url
                .host_str()
                .ok_or(Failure::InvalidInput)?
                .trim_matches(['[', ']']);
            let port = url.port_or_known_default().ok_or(Failure::InvalidInput)?;
            let addresses = tokio::net::lookup_host((host, port))
                .await
                .map_err(|_| Failure::Network)?
                .collect::<Vec<SocketAddr>>();
            if addresses.is_empty() || addresses.iter().any(|a| !public_ip(a.ip())) {
                return Err(Failure::InvalidInput);
            }
            // Pin the checked addresses for this hop; the HTTP client cannot re-resolve them.
            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .resolve_to_addrs(host, &addresses)
                .user_agent(concat!("telegram-archive-bot/", env!("CARGO_PKG_VERSION")))
                .build()
                .map_err(|_| Failure::Internal)?;
            let mut request = client.get(url.clone());
            if let Some(token) = bearer {
                if url.origin() != submitted.origin() {
                    return Err(Failure::InvalidInput);
                }
                request = request.bearer_auth(token);
            }
            let response = request.send().await.map_err(|e| {
                if e.is_timeout() {
                    Failure::Timeout
                } else {
                    Failure::Network
                }
            })?;
            let status = response.status();
            if matches!(status.as_u16(), 301 | 302 | 303 | 307 | 308) {
                if redirects.len() >= 5 {
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
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok())
                .unwrap_or("")
                .split(';')
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
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
                bytes,
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
}
