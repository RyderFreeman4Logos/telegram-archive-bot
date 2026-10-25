use clap::{Parser, Subcommand};
use telegram_archive_bot::config::Config;
use telegram_archive_bot::storage::Store;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    /// Capture one URL into canonical storage without Telegram polling or sending.
    #[arg(short, long)]
    url: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start durable workers, with Telegram polling only when enabled.
    Run,
    /// Plan a bounded inclusive local-date range, without provider collection.
    DailyBackfill {
        #[arg(long)]
        from: String,
        #[arg(long)]
        through: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// Inspect durable daily windows; continuation is the last returned window ID.
    DailyStatus {
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[arg(long)]
        after: Option<String>,
    },
    /// Retry one failed/blocked daily window, preserving cursor and history.
    DailyRetry { window_id: String },
    /// Inspect local tools, integrity and capabilities; never sends network requests.
    Doctor,
    /// Print local queue, retention, cache and projection metrics.
    Status,
    /// List bounded private-accounting counts and report states, without source bodies.
    FailureReportStatus {
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// List bounded publication identities and states without bodies or credentials.
    NotionList {
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Read portable retained-PDF metadata and availability; includes original URLs, no network.
    NotionPdfStatus {
        #[arg(long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        after: Option<String>,
    },
    /// Schedule read-only remote-body reconciliation; an optional page ID is untrusted input.
    NotionReconcile {
        id: String,
        #[arg(long)]
        page_id: Option<String>,
    },
    /// Re-arm a known no-effect failure; uncertain remote writes cannot be recreated.
    NotionRetry { id: String },
    /// Compute persisted updates without polling or sending; enabled providers may run.
    Replay { updates: std::path::PathBuf },
    /// Summarize read-only local files; with no paths, read paths from stdin.
    Summary {
        files: Vec<std::path::PathBuf>,
        #[arg(long)]
        stdin0: bool,
        #[arg(long)]
        rename: bool,
        #[arg(long)]
        auto_commit: bool,
        #[arg(long)]
        send_telegram: bool,
    },
    #[command(hide = true)]
    MediaEgressRelay {
        socket: std::path::PathBuf,
        executable: std::path::PathBuf,
        #[arg(long)]
        browser: bool,
        #[arg(last = true)]
        args: Vec<std::ffi::OsString>,
    },
    /// Validate configuration; never sends network requests.
    CheckConfig,
    /// Print a non-secret example configuration.
    ExampleConfig,
    /// Initialize local canonical storage without external side effects.
    Init,
    /// Verify SQLite constraints and every retained object digest.
    Verify,
    /// Project semantic records into the independently configured archive Git repository.
    Project {
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Create a verified SQLite/CAS backup in a new directory.
    Backup { destination: std::path::PathBuf },
    /// Verify a full backup without configuring or starting a bot.
    VerifyBackup { source: std::path::PathBuf },
    /// Restore a full backup to a new directory with external side effects paused.
    RestoreBackup {
        source: std::path::PathBuf,
        destination: std::path::PathBuf,
    },
    /// Rebuild committed semantic content; binaries and operational state remain unavailable.
    RestoreGit {
        source: std::path::PathBuf,
        destination: std::path::PathBuf,
    },
    /// Show whether a restored installation has a global recovery hold.
    RecoveryStatus,
    /// Clear only the global hold; old jobs/deliveries still require individual reconciliation.
    ReleaseRecoveryHold,
    /// Collect abandoned staging files and uncommitted objects after the grace period.
    Gc,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if let Some(url) = cli.url {
        anyhow::ensure!(
            cli.command.is_none(),
            "--url cannot be combined with a subcommand"
        );
        let config = Config::load(&cli.config.unwrap_or_else(Config::default_path))?;
        return batch_result(with_signals(|cancel| async move {
            telegram_archive_bot::batch::capture_url(&config, &url, &cancel).await
        })?);
    }
    match cli
        .command
        .ok_or_else(|| anyhow::anyhow!("provide a command or --url; see --help"))?
    {
        Command::Run => {
            let path = cli.config.unwrap_or_else(Config::default_path);
            with_signals(|cancel| telegram_archive_bot::runtime::serve(path, cancel))?;
        }
        Command::DailyBackfill {
            from,
            through,
            dry_run,
        } => {
            let config = Config::load(&cli.config.unwrap_or_else(Config::default_path))?;
            let from = from.parse::<chrono::NaiveDate>()?;
            let through = through.parse::<chrono::NaiveDate>()?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs() as i64;
            let preview =
                telegram_archive_bot::planner::preview_backfill(&config, from, through, now)?;
            let report = if dry_run {
                preview
            } else {
                Store::open(&config.state_dir)?.backfill_daily(&config, from, through, now)?
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Replay { updates } => {
            let config = Config::load(&cli.config.unwrap_or_else(Config::default_path))?;
            anyhow::ensure!(
                updates.metadata()?.len() <= config.limits.input_bytes,
                "replay input exceeds byte limit"
            );
            let updates: Vec<telegram_archive_bot::telegram::Update> =
                serde_json::from_slice(&std::fs::read(updates)?)
                    .map_err(|_| anyhow::anyhow!("invalid replay update JSON"))?;
            let result =
                tokio::runtime::Runtime::new()?.block_on(telegram_archive_bot::runtime::replay(
                    &config,
                    &updates,
                    &tokio_util::sync::CancellationToken::new(),
                ))?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        Command::Summary {
            mut files,
            stdin0,
            rename,
            auto_commit,
            send_telegram,
        } => {
            use std::{io::Read, os::unix::ffi::OsStringExt};
            let config = Config::load(&cli.config.unwrap_or_else(Config::default_path))?;
            anyhow::ensure!(
                !stdin0 || files.is_empty(),
                "--stdin0 takes paths only from stdin"
            );
            if files.is_empty() {
                let mut input = Vec::new();
                std::io::stdin()
                    .take(config.limits.input_bytes.saturating_add(1))
                    .read_to_end(&mut input)?;
                anyhow::ensure!(
                    input.len() as u64 <= config.limits.input_bytes,
                    "path list exceeds input limit"
                );
                files = input
                    .split(|b| *b == if stdin0 { 0 } else { b'\n' })
                    .filter(|path| !path.is_empty())
                    .map(|path| std::ffi::OsString::from_vec(path.to_vec()).into())
                    .collect();
            }
            batch_result(with_signals(|cancel| async move {
                telegram_archive_bot::batch::summarize(
                    &config,
                    files,
                    rename,
                    auto_commit,
                    send_telegram,
                    &cancel,
                )
                .await
            })?)?;
        }
        Command::MediaEgressRelay {
            socket,
            executable,
            browser,
            args,
        } => {
            let runtime = tokio::runtime::Runtime::new()?;
            let status = runtime.block_on(telegram_archive_bot::egress::relay(
                socket, executable, args, browser,
            ))?;
            std::process::exit(status);
        }
        Command::VerifyBackup { source } => println!(
            "{}",
            serde_json::to_string_pretty(&Store::verify_backup(&source)?)?
        ),
        Command::RestoreBackup {
            source,
            destination,
        } => println!(
            "{}",
            serde_json::to_string_pretty(&Store::restore_backup(&source, &destination)?)?
        ),
        Command::RestoreGit {
            source,
            destination,
        } => {
            let config = cli
                .config
                .map(|p| Config::load(&p))
                .transpose()?
                .unwrap_or_default();
            let runtime = tokio::runtime::Runtime::new()?;
            let receipt = runtime.block_on(Store::restore_git(
                &source,
                &destination,
                &config,
                &tokio_util::sync::CancellationToken::new(),
            ))?;
            println!("{}", serde_json::to_string_pretty(&receipt)?);
        }
        Command::ExampleConfig => println!("{}", toml::to_string_pretty(&Config::default())?),
        Command::CheckConfig => {
            let config = Config::load(&cli.config.unwrap_or_else(Config::default_path))?;
            config.check_secrets()?;
            println!("valid configuration: {}", config.fingerprint()?);
        }
        command => {
            let config = Config::load(&cli.config.unwrap_or_else(Config::default_path))?;
            let store = Store::open(&config.state_dir)?;
            match command {
                Command::Doctor => {
                    let result = tokio::runtime::Runtime::new()?.block_on(
                        telegram_archive_bot::operator::doctor(
                            &config,
                            &store,
                            &tokio_util::sync::CancellationToken::new(),
                        ),
                    )?;
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
                Command::Status => println!(
                    "{}",
                    serde_json::to_string_pretty(&telegram_archive_bot::operator::metrics(
                        &store
                    )?)?
                ),
                Command::DailyStatus { limit, after } => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_secs() as i64;
                    let windows = store.daily_status(limit, after.as_deref(), now)?;
                    println!(
                        "{}",
                        serde_json::to_string_pretty(
                            &serde_json::json!({"windows":windows,"external_effects_paused":store.recovery_paused()?,"timeline_provider":telegram_archive_bot::timeline::readiness(&config)})
                        )?
                    );
                }
                Command::DailyRetry { window_id } => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)?
                        .as_secs() as i64;
                    println!(
                        "{}",
                        serde_json::to_string_pretty(
                            &store.retry_daily_window(&config, &window_id, now)?
                        )?
                    );
                }
                Command::NotionList { limit } => println!(
                    "{}",
                    serde_json::to_string_pretty(&store.notion_list(limit)?)?
                ),
                Command::NotionPdfStatus { limit, after } => println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &store.notion_pdf_availability_after(limit, after.as_deref())?
                    )?
                ),
                Command::FailureReportStatus { limit } => println!(
                    "{}",
                    serde_json::to_string_pretty(&store.failure_report_status(limit)?)?
                ),
                Command::NotionReconcile { id, page_id } => {
                    store.reconcile_notion(&id, page_id.as_deref())?;
                    println!(
                        "{}",
                        serde_json::json!({"id":id,"scheduled":"read_only_reconciliation"})
                    );
                }
                Command::NotionRetry { id } => {
                    store.retry_notion(&id)?;
                    println!(
                        "{}",
                        serde_json::json!({"id":id,"scheduled":"known_no_effect_retry"})
                    );
                }
                Command::Init => println!(
                    "initialized schema {}",
                    telegram_archive_bot::storage::SCHEMA_VERSION
                ),
                Command::Verify => println!("verified {} retained blobs", store.integrity()?),
                Command::Project { limit } => {
                    let runtime = tokio::runtime::Runtime::new()?;
                    let status = runtime.block_on(store.project(
                        &config,
                        limit,
                        &tokio_util::sync::CancellationToken::new(),
                    ))?;
                    println!("{}", serde_json::to_string_pretty(&status)?);
                }
                Command::Backup { destination } => println!(
                    "{}",
                    serde_json::to_string_pretty(&store.backup(&destination)?)?
                ),
                Command::RecoveryStatus => {
                    println!("external_side_effects_paused={}", store.recovery_paused()?)
                }
                Command::ReleaseRecoveryHold => {
                    store.release_recovery_hold()?;
                    println!(
                        "global hold cleared; old jobs/deliveries remain individually paused or unknown"
                    );
                }
                Command::Gc => println!(
                    "collected {} abandoned files",
                    store.gc(std::time::Duration::from_secs(
                        config.limits.gc_grace_seconds
                    ))?
                ),
                _ => unreachable!(),
            }
        }
    }
    Ok(())
}

fn batch_result(result: telegram_archive_bot::batch::BatchResult) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(&result)?);
    anyhow::ensure!(
        result.failed == 0,
        "one or more inputs are incomplete; see JSON results"
    );
    Ok(())
}

fn with_signals<T, F, Fut>(operation: F) -> anyhow::Result<T>
where
    F: FnOnce(tokio_util::sync::CancellationToken) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    tokio::runtime::Runtime::new()?.block_on(async {
        let cancel = tokio_util::sync::CancellationToken::new();
        let token = cancel.clone();
        let signal = tokio::spawn(async move {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
            token.cancel();
            Ok::<_, anyhow::Error>(())
        });
        let result = operation(cancel).await;
        signal.abort();
        result
    })
}
