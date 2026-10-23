use clap::Parser;

#[derive(Parser)]
#[command(version, about)]
struct Cli {}

fn main() {
    Cli::parse();
    println!(
        "telegram-archive-bot: implementation in progress; see docs/implementation/issue-map.json"
    );
}
