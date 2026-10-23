use anyhow::Result;
use clap::Parser;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: std::path::PathBuf,
    #[arg(long)]
    url: String,
    #[arg(long)]
    relay: std::path::PathBuf,
    #[arg(long)]
    run: bool,
}
async fn run(args: Args) -> Result<()> {
    use anyhow::ensure;
    use serde_json::json;
    use telegram_archive_bot::{
        audio::{Profile, Repair},
        config::Config,
        delivery::Delivery,
        jobs::Phase,
        sources::{Capture, Content, ContentMetadata, FetchStatus, Identity},
        storage::Store,
        telegram::Update,
        transforms::DownloadRequest,
    };
    use tokio_util::sync::CancellationToken;
    ensure!(args.run, "requires --run for live source access");
    let config = Config::load(&args.config)?;
    ensure!(
        !config.telegram.enabled && !config.archive.push,
        "disable polling and push for acceptance"
    );
    ensure!(
        config.sources.media_enabled,
        "media adapter must be enabled"
    );
    ensure!(
        !config.state_dir.join("archive.sqlite").exists(),
        "requires fresh acceptance state"
    );
    telegram_archive_bot::network::source_url(&args.url)?;
    let chat = config
        .telegram
        .allowed_chats
        .first()
        .ok_or_else(|| anyhow::anyhow!("test chat required"))?;
    let user = config
        .telegram
        .allowed_users
        .first()
        .ok_or_else(|| anyhow::anyhow!("test user required"))?;
    let store = Store::open(&config.state_dir)?;
    let input: Update = serde_json::from_value(
        json!({"update_id":1,"message":{"message_id":1,"chat":{"id":chat},"from":{"id":user},"text":args.url}}),
    )?;
    store.ingest(&config, &input, &json!({"acceptance":"live-media-v1"}))?;
    let lease = store
        .claim_phase(Phase::Fetch)?
        .ok_or_else(|| anyhow::anyhow!("expected source job"))?;
    let revision = store
        .capture(&Capture {
            identity: Identity::from_url(&args.url)?,
            submitted_url: Some(args.url.clone()),
            content: Some(Content {
                body: "Audio source; no transcript claimed.".into(),
                metadata: ContentMetadata::default(),
            }),
            status: FetchStatus::Complete,
            extractor: "live-media-acceptance".into(),
            evidence: json!({"audio_only":true}),
            raw_digest: None,
            relations: vec![],
        })?
        .ok_or_else(|| anyhow::anyhow!("expected source revision"))?;
    let cancel = CancellationToken::new();
    let output = store
        .download_audio(
            &lease,
            &DownloadRequest {
                revision,
                url: args.url,
                playlist_limit: None,
                profile: Profile::AudioTrack,
                repair: Repair::Transcode,
            },
            &args.relay,
            &cancel,
        )
        .await?;
    let mut artifacts = vec![];
    let mut deliveries = vec![];
    for id in &output.artifacts {
        let a = store.artifact(id)?;
        artifacts.push(json!({"artifact_id":id,"sha256":a.blob_digest,"bytes":a.byte_length,"profile":a.effective_profile,"container":a.container,"codec":a.codec,"duration_ms":a.duration_ms,"full_decode":a.validation.full_decode}));
        deliveries.push(Delivery::Audio {
            artifact_id: id.clone(),
            title: format!("Live acceptance — {} seconds", a.duration_ms / 1000),
            performer: "Source recording".into(),
            caption: "Explicit live source acceptance".into(),
        });
    }
    let pending = store.enqueue_deliveries(&lease, &deliveries)?;
    store.finish_phase(
        &lease,
        &json!({"artifacts":output.artifacts,"pending_deliveries":pending}),
        None,
        false,
    )?;
    if config.archive.directory.is_some() {
        store.project(&config, 100, &cancel).await?;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"artifacts":artifacts,"telegram_messages_sent":0,"client_ui_verified":false})
        )?
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
    async fn requires_explicit_live_opt_in_before_config_or_network() {
        let error = run(Args {
            config: "/absent/config".into(),
            url: "https://example.org/audio.mp3".into(),
            relay: "/absent/relay".into(),
            run: false,
        })
        .await
        .unwrap_err();
        assert!(error.to_string().contains("requires --run"));
    }
}
