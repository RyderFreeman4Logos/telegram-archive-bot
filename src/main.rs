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
    /// Validate configuration; never sends network requests.
    CheckConfig,
    /// Print a non-secret example configuration.
    ExampleConfig,
    /// Initialize local canonical storage without external side effects.
    Init,
    /// Verify SQLite constraints and every retained object digest.
    Verify,
    /// Collect abandoned staging files and uncommitted objects after the grace period.
    Gc,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
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
