use clap::{Parser, Subcommand};
use telegram_archive_bot::config::Config;
use telegram_archive_bot::storage::Store;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long)]
    config: Option<std::path::PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start one live Telegram poller and durable workers (explicit activation).
    Run,
    /// Inspect local tools, integrity and capabilities; never sends network requests.
    Doctor,
    /// Print local queue, retention, cache and projection metrics.
    Status,
    /// Compute persisted updates without polling or sending; enabled providers may run.
    Replay { updates: std::path::PathBuf },
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
    match cli.command {
        Command::Run => {
            let path = cli.config.unwrap_or_else(Config::default_path);
            tokio::runtime::Runtime::new()?.block_on(async {
                let cancel = tokio_util::sync::CancellationToken::new();
                let token = cancel.clone();
                let signal = tokio::spawn(async move {
                    let mut term =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                    tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
                    token.cancel();
                    Ok::<_, anyhow::Error>(())
                });
                let result = telegram_archive_bot::runtime::serve(path, cancel).await;
                signal.abort();
                result
            })?;
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
