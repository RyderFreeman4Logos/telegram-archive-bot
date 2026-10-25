use telegram_archive_bot::config::{Config, ConfigManager, SecretRef};

#[test]
fn notion_defaults_enabled_without_resolving_auth() {
    let config: Config = toml::from_str("").unwrap();
    config.validate().unwrap();
    config.check_secrets().unwrap();
    let snapshot = serde_json::to_value(config).unwrap();
    assert_eq!(snapshot["notion"]["enabled"], true);
    assert!(snapshot["notion"]["token"].is_null());
    assert!(snapshot["notion"]["destination"].is_null());
}

#[test]
fn notion_bounds_are_typed_and_missing_credentials_stay_nonfatal() {
    let config: Config = toml::from_str(
        "[notion]\nenabled = true\n[notion.token]\nkind = 'file'\nname = '/missing/notion-token'\n",
    )
    .unwrap();
    config.validate().unwrap();
    config.check_secrets().unwrap(); // local intake is independent from Notion availability
    for field in [
        "request_seconds",
        "operation_seconds",
        "payload_bytes",
        "response_bytes",
        "record_bytes",
        "max_blocks",
        "max_requests",
        "max_attempts",
        "retry_seconds",
    ] {
        let text = format!("[notion]\n{field} = 0\n");
        assert!(
            toml::from_str::<Config>(&text).unwrap().validate().is_err(),
            "{field}"
        );
    }
    assert!(toml::from_str::<Config>("[notion]\ninvented_secret = 'no'\n").is_err());
}

#[test]
fn semantic_file_limit_defaults_and_requires_a_positive_count() {
    let default: Config = toml::from_str("[limits]\ninput_bytes = 10000000").unwrap();
    assert_eq!(default.limits.semantic_files, 100_000);
    let configured: Config = toml::from_str("[limits]\nsemantic_files = 200000").unwrap();
    configured.validate().unwrap();
    assert_eq!(configured.limits.semantic_files, 200_000);
    let zero: Config = toml::from_str("[limits]\nsemantic_files = 0").unwrap();
    assert!(zero.validate().is_err());
}

#[test]
fn snapshots_reload_and_unknown_keys() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("config.toml");
    let config = Config::default();
    let mut manager = ConfigManager::new(config.clone()).unwrap();
    let old = manager.snapshot();
    std::fs::write(&path, "invented = 'synthetic-secret'").unwrap();
    let error = manager.reload(&path).err().unwrap().to_string();
    assert!(!error.contains("synthetic-secret"));
    assert_eq!(
        manager.snapshot().fingerprint().unwrap(),
        old.fingerprint().unwrap()
    );
    let mut next = config;
    next.model.language = "en".into();
    std::fs::write(&path, toml::to_string(&next).unwrap()).unwrap();
    manager.reload(&path).unwrap();
    assert_eq!(old.model.language, "zh");
    assert_eq!(manager.snapshot().model.language, "en");
    assert_ne!(
        old.fingerprint().unwrap(),
        manager.snapshot().fingerprint().unwrap()
    );
}

#[test]
fn secrets_are_references_and_permissions_are_enforced() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("key");
    std::fs::write(&path, "SYNTHETIC_SENTINEL_SECRET").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let secret = SecretRef::File(path.clone());
    assert!(secret.read().is_err());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(secret.read().unwrap(), "SYNTHETIC_SENTINEL_SECRET");
    let mut config = Config::default();
    config.model.key = Some(secret);
    assert!(
        !serde_json::to_string(&config)
            .unwrap()
            .contains("SYNTHETIC_SENTINEL_SECRET")
    );
    assert!(!format!("{config:?}").contains("SYNTHETIC_SENTINEL_SECRET"));
}

#[test]
fn authorization_and_invalid_combinations() {
    let mut config = Config::default();
    assert!(!config.telegram.authorized(1, 2));
    config.telegram.allowed_chats = vec![1];
    config.telegram.allowed_users = vec![2];
    assert!(config.telegram.authorized(1, 2));
    assert!(!config.telegram.authorized(1, 3));
    config.synthesis.local = true;
    config
        .synthesis
        .speakers
        .insert("Alice".into(), "voice".into());
    assert!(config.validate().is_err());
    config.synthesis.speakers.clear();
    config.model.endpoint = "https://user:synthetic@example.test".into();
    assert!(config.validate().is_err());
}

#[test]
fn guest_enrichment_is_explicit_and_requires_an_article_detector_and_secret_reference() {
    let mut config = Config::default();
    assert!(!config.sources.x_guest_enabled && config.sources.x_guest_bearer.is_none());
    config.sources.x_guest_enabled = true;
    assert!(config.validate().is_err());
    config.sources.x_guest_bearer = Some(SecretRef::File("/missing/guest-client-key".into()));
    assert!(config.validate().is_err());
    config.sources.x_fallback_enabled = true;
    config.validate().unwrap();
    assert!(config.check_secrets().is_err());
}
