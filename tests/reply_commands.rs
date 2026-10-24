use anyhow::Result;
use serde_json::{Value, json};
use telegram_archive_bot::{
    config::Config,
    jobs::Phase,
    storage::Store,
    telegram::{Ingress, Message, Update, parse_requests},
};

fn message(command: &str, target: Value) -> Message {
    serde_json::from_value(json!({
        "message_id": 900, "chat": {"id": 20}, "from": {"id": 30},
        "text": command, "reply_to_message": target,
    }))
    .unwrap()
}

#[test]
fn requested_operation_is_frozen_in_each_job_without_other_transforms() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()?;
    let store = Store::open(directory.path())?;
    let mut config = Config {
        state_dir: directory.path().to_owned(),
        ..Default::default()
    };
    config.telegram.bot_id = 10;
    config.telegram.allowed_chats = vec![20];
    config.telegram.allowed_users = vec![30];
    config.model.enabled = true;
    config.translation.enabled = true;
    config.synthesis.enabled = true;
    for (i, (command, expected)) in [
        ("/translate", (false, true, false)),
        ("/tts", (false, false, true)),
        ("/summarize", (true, false, false)),
    ]
    .into_iter()
    .enumerate()
    {
        let update = Update {
            update_id: 70 + i as i64,
            message: Some(message(
                command,
                json!({"message_id": 12, "chat": {"id": 20}, "text": "Original text"}),
            )),
            edited_message: None,
        };
        assert_eq!(
            store.ingest(&config, &update, &json!({}))?,
            Ingress::Scheduled(1)
        );
        let lease = store.claim_phase(Phase::Fetch)?.unwrap();
        assert_eq!(
            (
                lease.config.model.enabled,
                lease.config.translation.enabled,
                lease.config.synthesis.enabled
            ),
            expected,
            "a requested operation must not invoke unrelated transformations"
        );
        assert_eq!(
            store.ingest(&config, &update, &json!({}))?,
            Ingress::Replayed
        );
        assert_eq!(
            lease.config.translation.language,
            config.translation.language
        );
    }
    assert!(config.model.enabled && config.translation.enabled && config.synthesis.enabled);
    Ok(())
}

#[test]
fn reply_commands_capture_historical_text_and_file_as_scoped_operations() -> Result<()> {
    let config = Config::default();
    for (command, operation) in [
        ("/translate", "translate"),
        ("/tts", "tts"),
        ("/summarize", "summarize"),
    ] {
        let historical =
            json!({"message_id": 12, "chat": {"id": 20}, "text": "Historical original text."});
        let requests = parse_requests(&message(command, historical), &config);
        assert!(
            requests.is_ok(),
            "reply operation must be admitted: {command}"
        );
        assert_eq!(
            serde_json::to_value(requests?)?,
            json!([{
                "kind": "transform", "operation": operation,
                "source": {"kind": "text", "text": "Historical original text."},
            }])
        );
    }
    let historical = json!({
        "message_id": 13, "chat": {"id": 20}, "caption": "https://example.com/source",
        "document": {"file_id": "telegram-file", "file_unique_id": "stable-file", "file_name": "old.md", "file_size": 48},
    });
    let requests = parse_requests(&message("/tts@archive_bot", historical), &config)?;
    assert_eq!(
        serde_json::to_value(requests)?,
        json!([{
            "kind": "transform", "operation": "tts",
            "source": {"kind": "document", "file_id": "telegram-file", "unique_id": "stable-file", "name": "old.md", "size": 48},
        }])
    );
    Ok(())
}

#[test]
fn reply_commands_reject_cross_chat_targets_and_commands_as_source() {
    let config = Config::default();
    for target in [
        json!({"message_id": 12, "chat": {"id": 99}, "text": "Other chat content"}),
        json!({"message_id": 12, "chat": {"id": 20}, "text": "/cancel secret-job"}),
        json!({"message_id": 12, "chat": {"id": 20}}),
    ] {
        assert!(parse_requests(&message("/translate", target), &config).is_err());
    }
}

#[test]
fn start_is_help_and_inline_operation_preserves_source_text() -> Result<()> {
    let config = Config::default();
    assert_eq!(
        serde_json::to_value(parse_requests(&message("/start", Value::Null), &config)?)?,
        json!([{"kind":"command","name":"help","argument":""}])
    );
    assert_eq!(
        serde_json::to_value(parse_requests(
            &message("/tts Read this original text.", Value::Null),
            &config
        )?)?,
        json!([{"kind":"transform","operation":"tts","source":{"kind":"text","text":"Read this original text."}}])
    );
    Ok(())
}
