//! Opt-in acceptance helper. It sends pending entries and never polls Telegram.
use anyhow::Result;
use clap::Parser;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: std::path::PathBuf,
    #[arg(long)]
    send: bool,
    #[arg(long, default_value_t = 10)]
    limit: usize,
}
async fn run(args: Args) -> Result<()> {
    use anyhow::ensure;
    use serde_json::{Value, json};
    use telegram_archive_bot::{
        config::Config, delivery::DeliveryWorker, storage::Store, telegram::Telegram,
    };
    use tokio_util::sync::CancellationToken;
    ensure!(
        args.send && (1..=100).contains(&args.limit),
        "requires --send and limit 1..100"
    );
    let config = Config::load(&args.config)?;
    config.check_secrets()?;
    let token = config
        .telegram
        .token
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("token reference required"))?
        .read()?;
    let store = Store::open(&config.state_dir)?;
    let worker = DeliveryWorker {
        store: store.clone(),
        telegram: Telegram::new(token)?,
        bot_id: config.telegram.bot_id,
    };
    let cancel = CancellationToken::new();
    let mut attempts = 0;
    while attempts < args.limit && worker.send_next(&cancel).await? {
        attempts += 1;
    }
    let conn = rusqlite::Connection::open_with_flags(
        store.root().join("archive.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let rows = conn
        .prepare("SELECT id,state,receipt FROM outbox ORDER BY rowid")?
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let receipts=rows.iter().map(|(id,state,raw)|{
        let receipt=raw.as_ref().and_then(|s|serde_json::from_str::<Value>(s).ok());
        json!({"delivery_id":id,"state":state,"returned_type":receipt.as_ref().map(|v|&v["media_type"]),"returned_duration_seconds":receipt.as_ref().map(|v|&v["duration_seconds"])})
    }).collect::<Vec<_>>();
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"attempts":attempts,"receipts":receipts,"client_ui_verified":false})
        )?
    );
    ensure!(
        !rows.is_empty() && rows.iter().all(|(_, state, _)| state == "confirmed"),
        "unconfirmed delivery remains; inspect without blind resend"
    );
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    run(Args::parse()).await
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn refuses_missing_authorization_and_unbounded_admission_before_reading_config() {
        for (send, limit) in [(false, 10), (true, 0), (true, 101)] {
            let result = run(Args {
                config: "/absent/config-must-not-be-read".into(),
                send,
                limit,
            })
            .await;
            assert!(
                result.is_err_and(|e| e.to_string().contains("requires --send and limit 1..100"))
            );
        }
    }
}
