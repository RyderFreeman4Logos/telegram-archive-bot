//! Media tools have an isolated network namespace; only this proxy opens host TCP sockets.
use crate::{
    jobs::Failure,
    network::{public_ip, source_url},
};
use anyhow::{Result, ensure};
#[cfg(test)]
use std::net::SocketAddr;
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixListener, UnixStream},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

pub(crate) struct Proxy {
    directory: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
    cancel: CancellationToken,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}
impl Proxy {
    pub(crate) fn start(bytes: u64, timeout: Duration) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("ta-eg-")
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir()?;
        let listener = UnixListener::bind(directory.path().join("proxy.sock"))?;
        fs::set_permissions(
            directory.path().join("proxy.sock"),
            fs::Permissions::from_mode(0o600),
        )?;
        let remaining = Arc::new(AtomicU64::new(bytes));
        let cancel = CancellationToken::new();
        let stopping = cancel.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _=stopping.cancelled()=>break,
                    accepted=listener.accept(), if connections.len()<16 => match accepted {
                        Ok((socket,_))=>{let remaining=remaining.clone();connections.spawn(async move {
                            let _=tokio::time::timeout(timeout,forward(socket,remaining,Route::default())).await;
                        });},
                        Err(_)=>break,
                    },
                    _=connections.join_next(), if !connections.is_empty()=>(),
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Ok(Self {
            directory,
            task,
            cancel,
        })
    }
    pub(crate) async fn close(mut self) -> Result<()> {
        self.cancel.cancel();
        (&mut self.task).await?;
        Ok(())
    }
    pub(crate) fn directory(&self) -> &Path {
        self.directory.path()
    }
    pub(crate) fn socket(&self) -> PathBuf {
        self.directory.path().join("proxy.sock")
    }
}

#[derive(Default)]
struct Route {
    #[cfg(test)]
    addresses: Option<Vec<SocketAddr>>,
    #[cfg(test)]
    connect: Option<SocketAddr>,
}
impl Route {
    async fn connect(&self, host: &str, port: u16) -> Result<TcpStream> {
        #[cfg(test)]
        let addresses = if let Some(addresses) = &self.addresses {
            addresses.clone()
        } else {
            tokio::net::lookup_host((host, port)).await?.collect()
        };
        #[cfg(not(test))]
        let addresses: Vec<_> = tokio::net::lookup_host((host, port)).await?.collect();
        ensure!(
            !addresses.is_empty()
                && addresses.len() <= 64
                && addresses.iter().all(|a| public_ip(a.ip())),
            Failure::InvalidInput
        );
        #[cfg(test)]
        if let Some(address) = self.connect {
            return Ok(TcpStream::connect(address).await?);
        }
        // Connect only the addresses just checked, never resolve again in the connector.
        Ok(tokio::time::timeout(
            Duration::from_secs(10),
            TcpStream::connect(addresses.as_slice()),
        )
        .await??)
    }
}

async fn limited_copy(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    remaining: Arc<AtomicU64>,
) -> Result<()> {
    let mut buf = [0u8; 16384];
    loop {
        let count = reader.read(&mut buf).await?;
        if count == 0 {
            writer.shutdown().await?;
            return Ok(());
        }
        ensure!(
            remaining
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n
                    .checked_sub(count as u64))
                .is_ok(),
            Failure::InvalidOutput
        );
        writer.write_all(&buf[..count]).await?;
    }
}

async fn forward(mut socket: UnixStream, remaining: Arc<AtomicU64>, route: Route) -> Result<()> {
    let mut data = vec![];
    let mut buffer = [0u8; 1024];
    let end = loop {
        let count = socket.read(&mut buffer).await?;
        ensure!(count > 0, Failure::InvalidInput);
        data.extend_from_slice(&buffer[..count]);
        if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            ensure!(end <= 16384, Failure::InvalidInput);
            break end + 4;
        }
        ensure!(data.len() <= 16384, Failure::InvalidInput);
    };
    let header = std::str::from_utf8(&data[..end]).map_err(|_| Failure::InvalidInput)?;
    let mut lines = header.split("\r\n");
    let first = lines.next().ok_or(Failure::InvalidInput)?;
    let words: Vec<_> = first.split(' ').collect();
    ensure!(
        words.len() == 3 && matches!(words[2], "HTTP/1.0" | "HTTP/1.1"),
        Failure::InvalidInput
    );
    let tunnel = words[0] == "CONNECT";
    let url = if tunnel {
        ensure!(
            !words[1].contains(['/', '?', '#', '@']),
            Failure::InvalidInput
        );
        source_url(&format!("https://{}/", words[1]))?
    } else {
        ensure!(
            matches!(words[0], "GET" | "HEAD" | "POST"),
            Failure::InvalidInput
        );
        source_url(words[1])?
    };
    ensure!(tunnel || url.scheme() == "http", Failure::InvalidInput);
    let mut upstream = match route
        .connect(
            url.host_str().unwrap().trim_matches(['[', ']']),
            url.port_or_known_default().unwrap(),
        )
        .await
    {
        Ok(upstream) => upstream,
        Err(error) => {
            socket
                .write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await?;
            return Err(error);
        }
    };
    if tunnel {
        socket
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await?;
    } else {
        let target = &url[url::Position::BeforePath..url::Position::AfterQuery];
        let authority = &url[url::Position::BeforeHost..url::Position::AfterPort];
        let mut request = format!(
            "{} {target} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n",
            words[0]
        );
        for line in lines.filter(|l| !l.is_empty()) {
            let (name, _) = line.split_once(':').ok_or(Failure::InvalidInput)?;
            ensure!(
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'),
                Failure::InvalidInput
            );
            if !matches!(
                name.to_ascii_lowercase().as_str(),
                "host" | "connection" | "proxy-connection" | "proxy-authorization"
            ) {
                request.push_str(line);
                request.push_str("\r\n");
            }
        }
        request.push_str("\r\n");
        upstream.write_all(request.as_bytes()).await?;
    }
    ensure!(
        remaining
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n
                .checked_sub(data.len() as u64))
            .is_ok(),
        Failure::InvalidOutput
    );
    upstream.write_all(&data[end..]).await?;
    let (client_read, client_write) = socket.split();
    let (remote_read, remote_write) = upstream.split();
    tokio::try_join!(
        limited_copy(client_read, remote_write, remaining.clone()),
        limited_copy(remote_read, client_write, remaining)
    )?;
    Ok(())
}

/// Internal executable entrypoint inside the already isolated network namespace.
pub async fn relay(
    socket: PathBuf,
    executable: PathBuf,
    args: Vec<std::ffi::OsString>,
    browser: bool,
) -> Result<i32> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = format!("http://{}", listener.local_addr()?);
    let mut command = tokio::process::Command::new(executable);
    if browser {
        command
            .arg(format!("--proxy-server={proxy}"))
            .arg("--proxy-bypass-list=<-loopback>");
    } else {
        command.arg("--proxy").arg(proxy);
    }
    let mut child = command.args(args).kill_on_drop(true).spawn()?;
    let mut connections = JoinSet::new();
    let status = loop {
        tokio::select! {
            result=child.wait()=>break result?,
            accepted=listener.accept(),if connections.len()<16=>{
                let (mut local,_)=accepted?;let path=socket.clone();
                connections.spawn(async move {let mut remote=UnixStream::connect(path).await?;tokio::io::copy_bidirectional(&mut local,&mut remote).await?;Ok::<_,std::io::Error>(())});
            },
            _=connections.join_next(),if !connections.is_empty()=>(),
        }
    };
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(status.code().unwrap_or(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn http_proxy_rewrites_authority_and_private_redirect_followup_is_denied() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let (mut client, proxy) = UnixStream::pair()?;
        let task = tokio::spawn(forward(
            proxy,
            Arc::new(AtomicU64::new(8192)),
            Route {
                addresses: Some(vec!["93.184.216.34:80".parse()?]),
                connect: Some(listener.local_addr()?),
            },
        ));
        client.write_all(b"GET http://example.org/path?query=value HTTP/1.1\r\nHost: untrusted.invalid\r\nProxy-Authorization: private-proxy-secret\r\n\r\n").await?;
        client.shutdown().await?;
        let (mut remote, _) = listener.accept().await?;
        let mut request = vec![];
        remote.read_to_end(&mut request).await?;
        let request = String::from_utf8(request)?;
        assert!(request.starts_with("GET /path?query=value HTTP/1.1\r\nHost: example.org\r\n"));
        assert!(!request.contains("private-proxy-secret"));
        assert!(!request.contains("untrusted.invalid"));
        remote.write_all(b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1/private\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
        drop(remote);
        let mut response = vec![];
        client.read_to_end(&mut response).await?;
        task.await??;
        assert!(response.starts_with(b"HTTP/1.1 302"));
        let (mut client, proxy) = UnixStream::pair()?;
        let task = tokio::spawn(forward(
            proxy,
            Arc::new(AtomicU64::new(8192)),
            Route::default(),
        ));
        client
            .write_all(b"GET http://127.0.0.1/private HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
            .await?;
        assert!(task.await?.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn proxy_pins_public_dns_blocks_mixed_private_and_bounds_tunnel_bytes() -> Result<()> {
        for mixed in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let mut addresses = vec!["93.184.216.34:443".parse()?];
            if mixed {
                addresses.push("127.0.0.1:443".parse()?);
            }
            let (mut client, proxy) = UnixStream::pair()?;
            let task = tokio::spawn(forward(
                proxy,
                Arc::new(AtomicU64::new(1024)),
                Route {
                    addresses: Some(addresses),
                    connect: Some(address),
                },
            ));
            client
                .write_all(b"CONNECT example.org:443 HTTP/1.1\r\nHost: example.org\r\n\r\n")
                .await?;
            let mut header = [0u8; 128];
            let n = client.read(&mut header).await?;
            if mixed {
                assert!(header[..n].starts_with(b"HTTP/1.1 403"));
                assert!(task.await?.is_err());
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), listener.accept())
                        .await
                        .is_err()
                );
            } else {
                assert!(header[..n].starts_with(b"HTTP/1.1 200"));
                let (mut server, _) = listener.accept().await?;
                server.write_all(&vec![42u8; 2048]).await?;
                drop(server);
                drop(client);
                assert!(task.await?.is_err());
            }
        }
        Ok(())
    }
}
