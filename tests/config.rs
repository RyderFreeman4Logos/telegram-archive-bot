use telegram_archive_bot::config::{Config, ConfigManager, SecretRef};

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
