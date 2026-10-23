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
    #[command(hide = true)]
    MediaEgressRelay {
        socket: std::path::PathBuf,
        executable: std::path::PathBuf,
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
        Command::MediaEgressRelay {
            socket,
            executable,
            args,
        } => {
            let runtime = tokio::runtime::Runtime::new()?;
            let status = runtime.block_on(telegram_archive_bot::egress::relay(
                socket, executable, args,
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
