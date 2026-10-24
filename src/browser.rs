use crate::{config::Config, egress::Proxy, jobs::Failure, process::Tool, storage::Store};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

fn arguments(
    config: &Config,
    work: &Path,
    proxy: &Proxy,
    relay: &Path,
    browser: &Path,
    url: &str,
) -> Vec<OsString> {
    let mut args: Vec<OsString> = ["--unshare-all", "--die-with-parent", "--new-session"]
        .into_iter()
        .map(Into::into)
        .collect();
    // Only system runtime files are visible; never mount a host home or root tree.
    for path in [
        "/usr",
        "/bin",
        "/lib",
        "/lib64",
        "/opt/google/chrome",
        "/etc/fonts",
        "/etc/ssl",
        "/etc/passwd",
        "/etc/group",
        "/etc/nsswitch.conf",
        "/etc/ld.so.cache",
    ] {
        if Path::new(path).exists() {
            args.extend(["--ro-bind".into(), path.into(), path.into()]);
        }
    }
    args.extend([
        "--proc".into(),
        "/proc".into(),
        "--dev".into(),
        "/dev".into(),
        "--size".into(),
        config.limits.work_bytes.to_string().into(),
        "--tmpfs".into(),
        "/tmp".into(),
        "--bind".into(),
        work.into(),
        "/work".into(),
        "--ro-bind".into(),
        proxy.directory().into(),
        "/proxy".into(),
        "--ro-bind".into(),
        relay.into(),
        "/app/archive-bot".into(),
        "--chdir".into(),
        "/work".into(),
        "--clearenv".into(),
        "--setenv".into(),
        "HOME".into(),
        "/tmp".into(),
        "--setenv".into(),
        "PATH".into(),
        "/usr/bin:/bin".into(),
        "--".into(),
        "/app/archive-bot".into(),
        "media-egress-relay".into(),
        "--browser".into(),
        "/proxy/proxy.sock".into(),
        browser.into(),
        "--".into(),
    ]);
    args.extend(
        [
            "--headless",
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-extensions",
            "--disable-quic",
            "--user-data-dir=/tmp/profile",
            "--dump-dom",
            "--virtual-time-budget=5000",
            url,
        ]
        .into_iter()
        .map(Into::into),
    );
    args
}

pub(crate) fn system_path(path: &Path) -> Result<PathBuf> {
    let path = path.canonicalize().map_err(|_| Failure::ToolMissing)?;
    ensure!(
        path.starts_with("/usr") || path.starts_with("/opt/google/chrome"),
        "browser must be installed in a supported system runtime directory"
    );
    Ok(path)
}

pub(crate) async fn render(
    store: &Store,
    config: &Config,
    url: &str,
    budget: Duration,
    cancel: &CancellationToken,
) -> Result<(Vec<u8>, Value)> {
    crate::network::source_url(url)?;
    execute(
        store,
        config,
        url,
        &std::env::current_exe()?,
        budget,
        cancel,
    )
    .await
}

async fn execute(
    store: &Store,
    config: &Config,
    url: &str,
    relay: &Path,
    budget: Duration,
    cancel: &CancellationToken,
) -> Result<(Vec<u8>, Value)> {
    ensure!(
        config.sources.browser_enabled && !budget.is_zero(),
        Failure::Timeout
    );
    let browser = system_path(&config.tools.browser)?;
    let identity = crate::transforms::executable(&browser)?;
    let work = store.workspace("browser")?;
    let proxy = Proxy::start(config.limits.work_bytes, budget)?;
    let tool = Tool {
        executable: config.tools.bubblewrap.clone(),
        args: arguments(config, work.path(), &proxy, relay, &browser, url),
        environment: BTreeMap::new(),
        directory: work.path().to_owned(),
        timeout: budget,
        log_bytes: config.limits.log_bytes,
        stdout_bytes: Some(usize::try_from(config.limits.input_bytes)?),
        work_bytes: config.limits.work_bytes,
        secrets: vec![],
    };
    let output = tool.run(cancel).await;
    proxy.close().await?;
    let output = output?;
    ensure!(!output.stdout.trim().is_empty(), Failure::InvalidOutput);
    let evidence = json!({"renderer":"chromium-headless-dump-dom-v1", "browser":identity,
        "supervisor_digest":output.executable_digest, "exit_code":output.exit_code,
        "http_status":null, "resolved_url":null, "redirects":null,
        "profile":"private-bounded-tmpfs", "host_profile_used":false});
    Ok((output.stdout.into_bytes(), evidence))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn namespace_arguments_exclude_host_state_and_direct_network() -> Result<()> {
        let config = Config::default();
        let proxy = Proxy::start(1024, Duration::from_secs(1))?;
        let args = arguments(
            &config,
            Path::new("/private/work"),
            &proxy,
            Path::new("/private/bot"),
            Path::new("/usr/bin/chromium"),
            "https://example.org/",
        );
        let args = args.iter().map(|a| a.to_str().unwrap()).collect::<Vec<_>>();
        assert!(args.contains(&"--unshare-all") && !args.contains(&"--share-net"));
        assert!(!args.windows(3).any(|w| w == ["--ro-bind", "/", "/"]));
        assert!(!args.contains(&"/home") && !args.contains(&"/run"));
        assert!(args.contains(&"--clearenv") && args.contains(&"--browser"));
        assert!(args.contains(&"--user-data-dir=/tmp/profile"));
        assert!(
            args.windows(4)
                .any(|w| w == ["--size", "1000000000", "--tmpfs", "/tmp"])
        );
        proxy.close().await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "explicit installed Chrome and current application relay required"]
    async fn installed_browser_executes_javascript_in_private_namespace() -> Result<()> {
        let root = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))?;
        let store = Store::open(root.path())?;
        let mut config = Config::default();
        config.sources.browser_enabled = true;
        config.tools.browser = std::env::var_os("ARCHIVE_BROWSER_EXECUTABLE")
            .ok_or_else(|| anyhow::anyhow!("browser fixture required"))?
            .into();
        let relay = std::env::var_os("ARCHIVE_BROWSER_RELAY")
            .ok_or_else(|| anyhow::anyhow!("current relay fixture required"))?;
        let html = "<html><body><main id='content'></main><script>document.getElementById('content').textContent='Rendered synthetic article';</script></body></html>";
        let url = format!(
            "data:text/html,{}",
            url::form_urlencoded::byte_serialize(html.as_bytes())
                .collect::<String>()
                .replace('+', "%20")
        );
        let (bytes, evidence) = execute(
            &store,
            &config,
            &url,
            Path::new(&relay),
            Duration::from_secs(20),
            &CancellationToken::new(),
        )
        .await?;
        assert!(
            std::str::from_utf8(&bytes)?
                .contains("<main id=\"content\">Rendered synthetic article</main>")
        );
        assert!(evidence["http_status"].is_null() && evidence["resolved_url"].is_null());
        assert_eq!(evidence["host_profile_used"], false);
        assert_eq!(std::fs::read_dir(store.root().join("work"))?.count(), 0);
        Ok(())
    }
}
