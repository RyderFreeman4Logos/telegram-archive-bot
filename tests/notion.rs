use std::{os::unix::fs::PermissionsExt, path::Path};
use telegram_archive_bot::{
    config::{Notion, NotionDestination, SecretRef},
    notion::{Adapter, Failure, Provenance, Record, RecordData},
    sources::FetchStatus,
};
use tokio_util::sync::CancellationToken;

fn record() -> Record {
    Record {
        schema_version: 1,
        id: "revision-1".into(),
        title: "完整原文 🌍".into(),
        captured_at: "2026-10-25T00:00:00Z".into(),
        original_urls: vec!["https://example.org/article?original=yes".into()],
        provenance: Provenance {
            provider: "fixture".into(),
            extractor: "synthetic-v1".into(),
            retrieved_at: "2026-10-25T00:00:00Z".into(),
            completeness: FetchStatus::Complete,
            original_bytes_sha256: Some("a".repeat(64)),
        },
        data: RecordData::Revision {
            source_id: "source-1".into(),
            revision_id: "revision-1".into(),
            authored_at: Some("2026-10-24T20:00:00Z".into()),
        },
        content: "原文 🌍 e\u{301}\t\u{0001}\n\n".repeat(19000),
    }
}

fn fixture(mode: &str) -> (tempfile::TempDir, Notion) {
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let executable = root.path().join("ntn.py");
    std::fs::write(&executable, include_str!("fixtures/fake_ntn.py")).unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(root.path().join("mode"), mode).unwrap();
    let secret = root.path().join("token");
    std::fs::write(&secret, "SYNTHETIC_NOTION_TOKEN").unwrap();
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    let config = Notion {
        executable: Some(executable),
        token: Some(SecretRef::File(secret)),
        destination: Some(NotionDestination::Page {
            id: "00000000-0000-0000-0000-000000000002".into(),
        }),
        payload_bytes: 16384,
        ..Notion::default()
    };
    (root, config)
}

fn calls(root: &Path) -> Vec<Vec<String>> {
    std::fs::read_to_string(root.join("calls"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

// S87 / #87: Notion can remove controls while accepting a successful write.
#[cfg(feature = "ntn")]
#[tokio::test]
async fn s87_controls_survive_real_adapter_readback_as_reversible_json() {
    let (root, config) = fixture("strip_controls");
    let mut record = record();
    let controls: String = (0u8..=0x9f)
        .map(char::from)
        .filter(|c| c.is_control())
        .collect();
    record.content =
        format!("原文 🌍 e\u{301} literal \\\\u000c \\\"\n{controls}\n\u{c}").repeat(40);
    record.title.push('\u{85}'); // metadata is also a returned JSON document
    let original = record.clone();
    let verified = Adapter::new(&config, root.path())
        .create(&record, &CancellationToken::new())
        .await
        .expect("control-containing canonical text must survive Notion's lossy text boundary");
    assert_eq!(record, original);
    assert_eq!(
        verified.content_sha256,
        telegram_archive_bot::digest(&record.content)
    );
    let remote: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("state.json")).unwrap()).unwrap();
    let blocks = remote["blocks"].as_array().unwrap();
    let texts: Vec<&str> = blocks
        .iter()
        .map(|b| {
            b[b["type"].as_str().unwrap()]["rich_text"][0]["text"]["content"]
                .as_str()
                .unwrap()
        })
        .collect();
    assert!(
        texts
            .iter()
            .all(|s| !s.chars().any(|c| c.is_control() && c != '\n' && c != '\t'))
    );
    let start = texts
        .iter()
        .position(|s| *s == "Captured text (managed; JSON string encoding)")
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&texts[2..start].concat()).unwrap();
    assert_eq!(metadata["content_encoding"], "json-string-v1");
    assert_eq!(metadata["canonical_version"], "notion-ordered-text-json-v2");
    assert_eq!(metadata["title"], record.title);
    let encoded = texts[start + 1..texts.len() - 2].concat();
    // Independent reader reconstructs the original UTF-8, not an echoed digest.
    let decoded: String = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, record.content);
    assert_eq!(
        telegram_archive_bot::digest(&decoded),
        verified.content_sha256
    );
    assert!(texts.iter().all(|s| s.encode_utf16().count() <= 2000));
    assert_eq!(
        calls(root.path())
            .iter()
            .filter(|args| args.iter().any(|a| a == "POST"))
            .count(),
        1
    );
}

// DA05 / #93: U+2028 loss was observed through actual ntn GET readback.
// U+2029 exercises the deliberately conservative separator boundary offline.
#[cfg(feature = "ntn")]
async fn separator_roundtrip(separator: char, metadata_only: bool) {
    let (root, config) = fixture("strip_separators");
    let mut record = record();
    record.title = format!("metadata {separator} preserved");
    record.content = if metadata_only {
        "ordinary body 🌍 e\u{301}\n\t literal \\u2028 \\u2029 \"".into()
    } else {
        format!("原文 🌍 e\u{301}\n\t literal \\u2028 \\u2029 \"{separator}").repeat(80)
    };
    let original = record.clone();
    let verified = Adapter::new(&config, root.path())
        .create(&record, &CancellationToken::new())
        .await
        .expect("separator-bearing body and metadata must survive lossy readback");
    assert_eq!(
        record, original,
        "canonical local body must remain unchanged"
    );
    let remote: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("state.json")).unwrap()).unwrap();
    let texts: Vec<&str> = remote["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| {
            b[b["type"].as_str().unwrap()]["rich_text"][0]["text"]["content"]
                .as_str()
                .unwrap()
        })
        .collect();
    assert!(texts.iter().all(|s| !s.contains(['\u{2028}', '\u{2029}'])));
    assert!(texts.iter().all(|s| s.encode_utf16().count() <= 2000));
    let start = texts
        .iter()
        .position(|s| *s == "Captured text (managed; JSON string encoding)")
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_str(&texts[2..start].concat()).unwrap();
    assert_eq!(metadata["canonical_version"], "notion-ordered-text-json-v2");
    assert_eq!(metadata["content_encoding"], "json-string-v1");
    assert_eq!(metadata["title"], record.title);
    let decoded: String =
        serde_json::from_str(&texts[start + 1..texts.len() - 2].concat()).unwrap();
    assert_eq!(
        decoded, record.content,
        "independent decode must recover every separator and literal escape"
    );
    assert_eq!(
        telegram_archive_bot::digest(&decoded),
        verified.content_sha256
    );
    assert_eq!(metadata["content_utf8_sha256"], verified.content_sha256);
    assert_eq!(
        calls(root.path())
            .iter()
            .filter(|args| args.iter().any(|a| a == "POST"))
            .count(),
        1
    );
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn s93_line_separator_body_and_metadata_roundtrip() {
    separator_roundtrip('\u{2028}', false).await;
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn s93_paragraph_separator_body_and_metadata_roundtrip() {
    separator_roundtrip('\u{2029}', false).await;
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn s93_line_separator_metadata_only_roundtrip() {
    separator_roundtrip('\u{2028}', true).await;
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn s93_paragraph_separator_metadata_only_roundtrip() {
    separator_roundtrip('\u{2029}', true).await;
}

#[tokio::test]
async fn s93_separator_expansion_is_bounded_before_dispatch() {
    for separator in ['\u{2028}', '\u{2029}'] {
        let mut record = record();
        record.content = separator.to_string().repeat(2000);
        let config = Notion {
            record_bytes: serde_json::to_vec(&record).unwrap().len() + 100,
            ..Notion::default()
        };
        assert_eq!(record.plan(&config).unwrap_err().state, Failure::Limit);
        #[cfg(feature = "ntn")]
        {
            let (root, mut ready) = fixture("strip_separators");
            ready.record_bytes = config.record_bytes;
            assert_eq!(
                Adapter::new(&ready, root.path())
                    .create(&record, &CancellationToken::new())
                    .await
                    .unwrap_err()
                    .state,
                Failure::Limit
            );
            assert!(calls(root.path()).is_empty());
        }
    }
}

#[tokio::test]
async fn s87_control_encoding_expansion_is_bounded_before_dispatch() {
    let mut record = record();
    record.content = "\u{85}".repeat(2000);
    let config = Notion {
        record_bytes: serde_json::to_vec(&record).unwrap().len() + 100,
        ..Notion::default()
    };
    assert_eq!(record.plan(&config).unwrap_err().state, Failure::Limit);
    #[cfg(feature = "ntn")]
    {
        let (root, mut ready) = fixture("strip_controls");
        ready.record_bytes = config.record_bytes;
        assert_eq!(
            Adapter::new(&ready, root.path())
                .create(&record, &CancellationToken::new())
                .await
                .unwrap_err()
                .state,
            Failure::Limit
        );
        assert!(calls(root.path()).is_empty());
    }
}

#[test]
fn s87_ordinary_text_keeps_exact_v1_blocks_and_digest() {
    use telegram_archive_bot::notion::ManagedBlock;
    let mut record = record();
    record.content = "原文 🌍 e\u{301}\t\n\n literal \\\\u000c".into();
    let plan = record.plan(&Notion::default()).unwrap();
    let mut metadata = serde_json::to_value(&record).unwrap();
    metadata.as_object_mut().unwrap().remove("content");
    metadata["content_utf8_sha256"] =
        serde_json::json!(telegram_archive_bot::digest(&record.content));
    metadata["canonical_version"] = serde_json::json!("notion-ordered-text-v1");
    let block = |kind: &str, text: String| ManagedBlock {
        kind: kind.into(),
        text,
    };
    let expected = vec![
        block(
            "paragraph",
            format!("ARCHIVE_RECORD_V1 {}", plan.publication_key),
        ),
        block("heading_2", "Record metadata (managed)".into()),
        block(
            "paragraph",
            serde_json::to_string_pretty(&metadata).unwrap(),
        ),
        block("heading_2", "Captured text (managed)".into()),
        block("paragraph", record.content.clone()),
        block(
            "paragraph",
            format!("END_ARCHIVE_RECORD_V1 {}", plan.publication_key),
        ),
        block(
            "heading_2",
            "Human annotations and downstream analysis (unmanaged)".into(),
        ),
    ];
    assert_eq!(plan.canonical_version, "notion-ordered-text-v1");
    assert_eq!(plan.managed_blocks, expected);
    assert_eq!(
        plan.canonical_digest,
        telegram_archive_bot::digest(serde_json::to_vec(&expected).unwrap())
    );
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn publishes_all_unicode_then_verifies_returned_blocks() {
    let (root, config) = fixture("annotations");
    let record = record();
    let adapter = Adapter::new(&config, root.path());
    let verified = adapter
        .create(&record, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        verified.content_sha256,
        telegram_archive_bot::digest(&record.content)
    );
    assert_eq!(verified.schema_version, 1);
    let again = adapter
        .verify(&verified.page_id, &record, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(verified, again);
    let calls = calls(root.path());
    assert!(
        calls
            .iter()
            .filter(|a| a.iter().any(|v| v == "PATCH"))
            .count()
            > 1
    );
    assert!(
        calls
            .iter()
            .any(|a| a.iter().any(|v| v.starts_with("start_cursor==")))
    );
    assert_eq!(
        calls
            .iter()
            .filter(|a| a.iter().any(|v| v == "POST"))
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_dir(root.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().unwrap().is_dir())
            .count(),
        0
    );
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn returned_body_and_structure_must_match_even_with_unchanged_digest_markers() {
    for mode in ["corrupt_body", "omit", "unexpected", "truncate"] {
        let (root, config) = fixture(mode);
        let error = Adapter::new(&config, root.path())
            .create(&record(), &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.state, Failure::Conflict, "{mode}");
        assert!(
            error.page_id.is_some(),
            "created page must remain reconcilable"
        );
        assert_eq!(
            calls(root.path())
                .iter()
                .filter(|a| a.iter().any(|v| v == "POST"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn disabled_or_compiled_out_never_probes_even_with_valid_settings() {
    let (root, mut config) = fixture("annotations");
    config.enabled = false;
    assert_eq!(
        Adapter::new(&config, root.path())
            .create(&record(), &CancellationToken::new())
            .await
            .unwrap_err()
            .state,
        Failure::Disabled
    );
    assert!(calls(root.path()).is_empty());
    #[cfg(not(feature = "ntn"))]
    {
        config.enabled = true;
        assert_eq!(
            Adapter::new(&config, root.path())
                .create(&record(), &CancellationToken::new())
                .await
                .unwrap_err()
                .state,
            Failure::CompiledOut
        );
        assert!(calls(root.path()).is_empty());
    }
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn secret_spanning_chunk_boundary_is_rejected_before_publication() {
    let (root, config) = fixture("annotations");
    let mut record = record();
    record.content = "x".repeat(1990) + "SYNTHETIC_NOTION_TOKEN";
    let error = Adapter::new(&config, root.path())
        .create(&record, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.state, Failure::InvalidPayload);
    assert!(
        !calls(root.path())
            .iter()
            .any(|a| a.iter().any(|v| v == "POST"))
    );
    assert!(!format!("{error:?}").contains("SYNTHETIC_NOTION_TOKEN"));
}

#[tokio::test]
async fn doctor_exposes_nonfatal_notion_readiness() {
    use telegram_archive_bot::{
        config::{Config, Tools},
        operator,
        storage::Store,
    };
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let config = Config {
        tools: Tools {
            ffmpeg: "/missing/ffmpeg".into(),
            ffprobe: "/missing/ffprobe".into(),
            yt_dlp: "/missing/yt-dlp".into(),
            md2audio: "/missing/md2audio".into(),
            pdfinfo: "/missing/pdfinfo".into(),
            pdftotext: "/missing/pdftotext".into(),
            git: "/missing/git".into(),
            hymt_bridge: "/missing/bridge".into(),
            bubblewrap: "/missing/bwrap".into(),
            browser: "/missing/browser".into(),
        },
        ..Config::default()
    };
    let store = Store::open(root.path().join("store")).unwrap();
    let report = operator::doctor(&config, &store, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(report["ready"], true);
    assert_eq!(
        report["notion"]["state"],
        if cfg!(feature = "ntn") {
            "pending_configuration"
        } else {
            "compiled_out"
        }
    );
    assert_eq!(report["notion"]["network_probes_performed"], false);
}

#[test]
fn da01_unknown_account_and_time_evidence_survive_the_public_schema() {
    let account = serde_json::json!({
        "account": {"identity":"unresolved","provider":"x","key":"discovery-scope-1"},
        "handle":null,"display_name":null,"retrieved_at":1792886400,
        "provenance":{"identity_available":false}
    });
    let activity = serde_json::json!({
        "schema_version":1,"id":"observation-1",
        "activity":{
            "identity":{"identity":"unresolved","provider":"x","discovery_run_id":"run-1","occurrence_key":"page-2-slot-3"},
            "actor":account["account"],"kind":"repost",
            "source":{"provider":"x","external_id":"123"}
        },
        "account_observation":account,
        "discovery":{"provider":"x","key":"run-1","provenance":{"cursor":"synthetic"}},
        "observation_key":"observation-1","input_digest":"synthetic-digest",
        "source_observation_id":1,"view_id":"view-1","revision_id":null,"missing_reason":"unavailable",
        "activity_time":{"certainty":"unknown","provenance":{"upstream_omitted_time":true}},
        "source_time":{"certainty":"interval","earliest":{"seconds":1700000000,"nanoseconds":123},"latest":{"seconds":1700000001,"nanoseconds":456},"precision":"nanosecond","provenance":{"provider_precision":"retained"}},
        "retrieved_at":1792886400,"fetch_status":"partial","provenance":{"uncertain":true}
    });
    for (kind, observation) in [("account", account), ("activity", activity)] {
        let mut wire = serde_json::to_value(record()).unwrap();
        wire["data"] =
            serde_json::json!({"kind":kind,"observation":{"schema_version":1,"value":observation}});
        let loaded = serde_json::from_value::<Record>(wire.clone());
        assert!(
            loaded.is_ok(),
            "{kind}: preserve DA01 uncertainty without manufacturing identities or times"
        );
        let record = loaded.unwrap();
        assert_eq!(serde_json::to_value(&record).unwrap(), wire);
        let plan = record.plan(&Notion::default()).unwrap();
        let visible = plan
            .managed_blocks
            .iter()
            .map(|b| b.text.as_str())
            .collect::<String>();
        assert!(visible.contains("unresolved"));
        if kind == "activity" {
            assert!(
                visible.contains("unknown")
                    && visible.contains("nanosecond")
                    && visible.contains("1700000000")
            );
        }
    }
}

fn assert_persisted_da01_projection(known: bool) {
    use serde_json::json;
    use telegram_archive_bot::{
        activities::{
            AccountIdentity, ActivityCapture, ActivityIdentity, ActivityKind, ActivityObservation,
            DiscoveryRun, TimeEvidence, TimePrecision, Timestamp,
        },
        notion::Da01Observation,
        sources::{Capture, Content, ContentMetadata, Identity, ViewPolicy},
        storage::Store,
    };
    let root = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let store = Store::open(root.path()).unwrap();
    let mut input = ActivityCapture {
        actor: if known {
            AccountIdentity::ProviderId {
                provider: "x".into(),
                user_id: "42".into(),
            }
        } else {
            AccountIdentity::Unresolved {
                provider: "x".into(),
                key: "discovery-scope/unknown-actor".into(),
            }
        },
        handle: known.then(|| "observed_handle".into()),
        display_name: known.then(|| "名字 🌍".into()),
        provider_event_id: known.then(|| "9001".into()),
        kind: ActivityKind::Repost,
        discovery: DiscoveryRun {
            provider: "x".into(),
            key: "synthetic-run".into(),
            provenance: json!({"cursor":"page-2","raw_field":"原始证据"}),
        },
        occurrence_key: "position-3".into(),
        observation_key: "retrieval-1".into(),
        activity_time: if known {
            TimeEvidence::Instant {
                timestamp: Timestamp {
                    seconds: 1792886400,
                    nanoseconds: 123456789,
                },
                precision: TimePrecision::Nanosecond,
                provenance: json!({"provider_field":"event_at","raw":"1792886400.123456789"}),
            }
        } else {
            TimeEvidence::Unknown {
                provenance: json!({"omitted":true,"source_time_is_not_event_time":true}),
            }
        },
        source_time: TimeEvidence::Interval {
            earliest: Timestamp {
                seconds: 1792800000,
                nanoseconds: 987654321,
            },
            latest: Timestamp {
                seconds: 1792800001,
                nanoseconds: 123456789,
            },
            precision: TimePrecision::Nanosecond,
            provenance: json!({"evidence":["lower-bound","upper-bound"],"precision":"not-rounded"}),
        },
        retrieved_at: 1792886500,
        provenance: json!({"fixture":true,"source_path":"synthetic timeline","count":7}),
        capture: Capture {
            identity: Identity::x("100").unwrap(),
            submitted_url: Some("https://x.com/synthetic/status/100".into()),
            content: known.then(|| Content {
                body: "原始内容 🌍\n".into(),
                metadata: ContentMetadata::default(),
            }),
            status: if known {
                FetchStatus::Complete
            } else {
                FetchStatus::Missing
            },
            extractor: "da05-typed-fixture".into(),
            evidence: json!({"capture":"original"}),
            raw_digest: None,
            relations: vec![],
        },
        view_policy: ViewPolicy {
            max_nodes: 10,
            max_depth: 5,
        },
    };
    let original = store.record_activity(&input).unwrap();
    assert_eq!(original.activity.actor, input.actor);
    assert_eq!(original.activity_time, input.activity_time);
    assert_eq!(original.source_time, input.source_time);
    assert_eq!(original.revision_id.is_some(), known);
    assert_eq!(original.missing_reason.is_some(), !known);
    assert_eq!(
        matches!(
            original.activity.identity,
            ActivityIdentity::ProviderId { .. }
        ),
        known
    );
    // A later edit/hydration must not replace the older observation's revision or missing pin.
    input.observation_key = "retrieval-2".into();
    input.capture.content = Some(Content {
        body: "later revision".into(),
        metadata: ContentMetadata::default(),
    });
    input.capture.status = FetchStatus::Complete;
    let later = store.record_activity(&input).unwrap();
    assert_ne!(original.revision_id, later.revision_id);
    assert_eq!(
        original.activity.id().unwrap(),
        later.activity.id().unwrap()
    );
    drop(store);
    let reopened = Store::open(root.path()).unwrap();
    let observed = reopened.activity_observation(&original.id).unwrap();
    assert_eq!(observed, original);
    let account = reopened
        .account_history(&observed.activity.actor.id().unwrap())
        .unwrap();
    assert!(account.contains(&observed.account_observation));

    let account_envelope = Da01Observation::from_account(&observed.account_observation);
    let activity_envelope = Da01Observation::from_activity(&observed);
    assert!(
        account_envelope.is_ok() && activity_envelope.is_ok(),
        "actual DA01 conversions: account={account_envelope:?}; activity={activity_envelope:?}"
    );
    let candidates = [
        (
            observed.account_observation.id().unwrap(),
            RecordData::Account {
                observation: account_envelope.unwrap(),
            },
            serde_json::to_value(&observed.account_observation).unwrap(),
        ),
        (
            observed.id.clone(),
            RecordData::Activity {
                observation: activity_envelope.unwrap(),
            },
            serde_json::to_value(&observed).unwrap(),
        ),
    ];
    for (id, data, expected) in candidates {
        let mut publication = record();
        publication.id = id;
        publication.data = data;
        publication.provenance.completeness = observed.fetch_status;
        publication.content = if known {
            "原始内容 🌍\n".into()
        } else {
            String::new()
        };
        let plan = publication.plan(&Notion::default()).unwrap();
        let content_start = plan
            .managed_blocks
            .iter()
            .position(|b| b.text == "Captured text (managed)")
            .unwrap();
        let metadata: String = plan.managed_blocks[2..content_start]
            .iter()
            .map(|b| b.text.as_str())
            .collect();
        let visible: serde_json::Value = serde_json::from_str(&metadata).unwrap();
        assert_eq!(visible["id"], publication.id);
        assert_eq!(visible["data"]["observation"]["schema_version"], 1);
        assert_eq!(visible["data"]["observation"]["value"], expected);
        assert_eq!(
            visible["content_utf8_sha256"],
            telegram_archive_bot::digest(&publication.content)
        );
        assert_eq!(
            publication
                .plan(&Notion::default())
                .unwrap()
                .publication_key,
            plan.publication_key
        );
        if matches!(publication.data, RecordData::Activity { .. }) {
            let decoded: ActivityObservation =
                serde_json::from_value(visible["data"]["observation"]["value"].clone()).unwrap();
            assert_eq!(decoded, observed); // Includes event/account/source IDs, precision, provenance and all pins.
            assert_eq!(decoded.view_id, original.view_id);
            assert_eq!(
                decoded.source_observation_id,
                original.source_observation_id
            );
            assert_eq!(decoded.revision_id, original.revision_id);
        }
    }
}

#[test]
fn da01_known_store_observation_reaches_notion_without_loss() {
    assert_persisted_da01_projection(true);
}

#[test]
fn da01_unresolved_store_observation_reaches_notion_without_loss() {
    assert_persisted_da01_projection(false);
}

#[test]
fn schema_chunks_and_immutable_keys_preserve_every_character() {
    let record = record();
    let plan = record.plan(&Notion::default()).unwrap();
    let start = plan
        .managed_blocks
        .iter()
        .position(|b| b.text == "Captured text (managed; JSON string encoding)")
        .unwrap()
        + 1;
    let end = plan.managed_blocks.len() - 2;
    assert_eq!(
        serde_json::from_str::<String>(
            &plan.managed_blocks[start..end]
                .iter()
                .map(|b| b.text.as_str())
                .collect::<String>()
        )
        .unwrap(),
        record.content
    );
    assert!(
        plan.managed_blocks
            .iter()
            .all(|b| b.text.encode_utf16().count() <= 2000)
    );
    assert_eq!(record.plan(&Notion::default()).unwrap(), plan);
    let mut changed = record.clone();
    changed.content.push_str(" edited");
    let changed_plan = changed.plan(&Notion::default()).unwrap();
    assert_eq!(changed_plan.publication_key, plan.publication_key);
    assert_ne!(changed_plan.canonical_digest, plan.canonical_digest);
    changed.id = "revision-2".into();
    if let RecordData::Revision { revision_id, .. } = &mut changed.data {
        *revision_id = changed.id.clone();
    }
    assert_ne!(
        changed.plan(&Notion::default()).unwrap().publication_key,
        plan.publication_key
    );
    let mut future = record;
    future.schema_version = 2;
    assert_eq!(
        future.plan(&Notion::default()).unwrap_err().state,
        Failure::InvalidPayload
    );
}

#[test]
fn readiness_is_nonfatal_explicit_and_has_no_cli_side_effect() {
    use telegram_archive_bot::notion::{Prerequisite, ReadinessState, readiness};
    let (root, mut config) = fixture("annotations");
    if cfg!(feature = "ntn") {
        assert_eq!(
            readiness(&Notion::default()).state,
            ReadinessState::PendingConfiguration
        );
        assert_eq!(readiness(&config).state, ReadinessState::Ready);
        config.destination = None;
        assert!(
            readiness(&config)
                .missing
                .contains(&Prerequisite::Destination)
        );
        config.token = Some(SecretRef::File(root.path().join("absent-secret")));
        assert!(
            readiness(&config)
                .missing
                .contains(&Prerequisite::TokenUnavailable)
        );
        config.executable = Some(root.path().join("absent-executable"));
        assert!(
            readiness(&config)
                .missing
                .contains(&Prerequisite::Executable)
        );
    } else {
        assert_eq!(readiness(&config).state, ReadinessState::CompiledOut);
    }
    config.enabled = false;
    assert_eq!(readiness(&config).state, ReadinessState::Disabled);
    assert!(calls(root.path()).is_empty());
    assert!(
        !serde_json::to_string(&config)
            .unwrap()
            .contains("SYNTHETIC_NOTION_TOKEN")
    );
}

#[test]
fn remaining_record_kinds_keep_their_reader_visible_references() {
    use telegram_archive_bot::{
        notion::{Coverage, RecordLink},
        sources::RelationKind,
    };
    let link = RecordLink {
        record_id: "record-1".into(),
        source_id: "source-1".into(),
        revision_id: Some("revision-1".into()),
        original_url: "https://example.org/original".into(),
        notion_page_id: None,
    };
    for data in [
        RecordData::Source {
            provider: "web".into(),
            external_id: link.original_url.clone(),
        },
        RecordData::Relation {
            source_id: "source-1".into(),
            target_source_id: "source-2".into(),
            relation: RelationKind::Quotes,
        },
        RecordData::DailyManifest {
            local_date: "2026-10-24".into(),
            timezone: "America/Los_Angeles".into(),
            start_utc: "2026-10-24T07:00:00Z".into(),
            end_utc: "2026-10-25T07:00:00Z".into(),
            account_ids: vec!["account-1".into()],
            records: vec![link.clone()],
            coverage: Coverage {
                complete: false,
                admitted: 2,
                published: 1,
                missing_record_ids: vec!["record-2".into()],
                gaps: vec!["provider omission".into()],
            },
        },
        RecordData::AgentOutput {
            manifest_id: "manifest-1".into(),
            sop_version: "x-daily-briefing-v1/1.0.0".into(),
            level: 5,
            sources: vec![link],
        },
        RecordData::Checkpoint {
            manifest_id: "manifest-1".into(),
            sop_version: "x-daily-briefing-v1/1.0.0".into(),
            completed_levels: vec![1, 2],
            remaining_record_ids: vec!["record-2".into()],
        },
    ] {
        let mut record = record();
        record.data = data;
        record.content = "reader-visible record".into();
        let bytes = serde_json::to_vec(&record).unwrap();
        assert_eq!(serde_json::from_slice::<Record>(&bytes).unwrap(), record);
        let plan = record.plan(&Notion::default()).unwrap();
        assert!(
            plan.managed_blocks
                .iter()
                .any(|b| b.text.contains(record.data.kind()))
        );
    }
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn escaped_credential_values_are_rejected_before_publication() {
    let (root, config) = fixture("annotations");
    let token = "SYNTHETIC_\"\\_TOKEN";
    std::fs::write(root.path().join("token"), token).unwrap();
    let mut record = record();
    record.content = format!("credential: {token}");
    let error = Adapter::new(&config, root.path())
        .create(&record, &CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.state, Failure::InvalidPayload);
    assert!(
        !calls(root.path())
            .iter()
            .any(|a| a.iter().any(|v| v == "POST"))
    );
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn errors_are_bounded_classified_and_do_not_leak_or_retry_creation() {
    for (mode, expected) in [
        ("auth", Failure::BlockedAuth),
        ("rate", Failure::RetryWait),
        ("malformed", Failure::Reconcile),
        ("bad_version", Failure::UnsupportedTool),
        ("timeout", Failure::Reconcile),
    ] {
        let (root, mut config) = fixture(mode);
        config.request_seconds = 1;
        let mut record = record();
        record.content = "bounded fixture".into();
        let error = Adapter::new(&config, root.path())
            .create(&record, &CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.state, expected, "{mode}");
        if mode == "rate" {
            assert_eq!(error.retry_after_seconds, Some(7));
        }
        assert!(
            !serde_json::to_string(&error)
                .unwrap()
                .contains("SYNTHETIC_NOTION_TOKEN")
        );
        assert!(
            calls(root.path())
                .iter()
                .filter(|a| a.iter().any(|v| v == "POST"))
                .count()
                <= 1
        );
    }
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn data_source_parent_is_explicit_and_limits_fail_before_writes() {
    let (root, mut config) = fixture("annotations");
    config.destination = Some(NotionDestination::DataSource {
        id: "00000000000000000000000000000003".into(),
        title_property: "Name".into(),
    });
    let mut record = record();
    record.content = "data source original".into();
    Adapter::new(&config, root.path())
        .create(&record, &CancellationToken::new())
        .await
        .unwrap();
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.path().join("state.json")).unwrap()).unwrap();
    assert_eq!(
        state["parent"]["data_source_id"],
        "00000000-0000-0000-0000-000000000003"
    );
    assert!(state["properties"]["Name"]["title"].is_array());
    let (fresh, mut limited) = fixture("annotations");
    limited.record_bytes = 1;
    assert_eq!(
        Adapter::new(&limited, fresh.path())
            .create(&record, &CancellationToken::new())
            .await
            .unwrap_err()
            .state,
        Failure::Limit
    );
    assert!(calls(fresh.path()).is_empty());
    limited.record_bytes = 10000;
    limited.max_requests = 1;
    assert_eq!(
        Adapter::new(&limited, fresh.path())
            .create(&record, &CancellationToken::new())
            .await
            .unwrap_err()
            .state,
        Failure::Limit
    );
    assert!(calls(fresh.path()).is_empty());
}

#[cfg(feature = "ntn")]
#[tokio::test]
async fn cancellation_after_create_dispatch_requires_reconciliation() {
    let (root, config) = fixture("timeout");
    let cancel = CancellationToken::new();
    let child_cancel = cancel.clone();
    let adapter = Adapter::new(&config, root.path());
    let mut record = record();
    record.content = "cancel boundary".into();
    let task = tokio::spawn(async move { adapter.create(&record, &child_cancel).await });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if calls(root.path())
            .iter()
            .any(|a| a.iter().any(|v| v == "POST"))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "fixture never observed create dispatch"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    cancel.cancel();
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.state, Failure::Reconcile);
    assert_eq!(
        calls(root.path())
            .iter()
            .filter(|a| a.iter().any(|v| v == "POST"))
            .count(),
        1
    );
}
