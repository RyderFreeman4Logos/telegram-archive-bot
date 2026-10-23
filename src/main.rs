use clap::{Parser, Subcommand};
use telegram_archive_bot::config::Config;

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
    }
    Ok(())
}
