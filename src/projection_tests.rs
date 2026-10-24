use super::*;
use crate::sources::*;
use std::{os::unix::fs::symlink, process::Command};

fn git(root: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("/usr/bin/git")
        .arg("-C")
        .arg(root)
        .args(args)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .output()?;
    ensure!(
        output.status.success(),
        "fixture git {:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?
        .trim_end_matches('\n')
        .into())
}
fn fixture() -> Result<(tempfile::TempDir, Store, Config)> {
    let dir = tempfile::Builder::new()
        .prefix("archive-git-")
        .permissions(fs::Permissions::from_mode(0o700))
        .tempdir_in("/var/tmp")?;
    let state = dir.path().join("state");
    let store = Store::open(&state)?;
    let mut config = Config {
        state_dir: state,
        ..Config::default()
    };
    config.archive.directory = Some(dir.path().join("archive"));
    config.archive.branch = Some("main".into());
    Ok((dir, store, config))
}
fn capture(id: &str, text: &str) -> Capture {
    Capture {
        identity: Identity::x(id).unwrap(),
        submitted_url: Some(format!("https://twitter.com/a/status/{id}")),
        content: Some(Content {
            body: text.into(),
            metadata: ContentMetadata::default(),
        }),
        status: FetchStatus::Complete,
        extractor: "synthetic".into(),
        evidence: json!({"fixture":true}),
        raw_digest: None,
        relations: vec![],
    }
}
fn snapshot(root: &Path) -> Result<BTreeMap<String, Vec<u8>>> {
    fn visit(base: &Path, dir: &Path, result: &mut BTreeMap<String, Vec<u8>>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                visit(base, &entry.path(), result)?;
            } else {
                result.insert(
                    entry.path().strip_prefix(base)?.to_string_lossy().into(),
                    fs::read(entry.path())?,
                );
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    visit(root, &root.join(PREFIX), &mut files)?;
    Ok(files)
}
#[tokio::test]
async fn bounded_index_updates_preserve_exact_entries_and_unrelated_staging() -> Result<()> {
    let (dir, _store, mut config) = fixture()?;
    let archive = config.archive.directory.clone().unwrap();
    fs::create_dir(&archive)?;
    git(&archive, &["init", "-b", "main"])?;
    fs::write(archive.join("unrelated.txt"), "staged private note")?;
    git(&archive, &["add", "unrelated.txt"])?;
    let original_index = fs::read(archive.join(".git/index"))?;
    let unrelated = git(&archive, &["ls-files", "--stage"])?;
    let oid = git(&archive, &["hash-object", "-w", "unrelated.txt"])?;
    let wrapper = dir.path().join("git-wrapper");
    fs::write(
        &wrapper,
        "#!/bin/sh\nfor arg do\n  if [ \"$arg\" = update-index ]; then\n    printf 'update-index\\n' >> \"$0.calls\"\n  fi\ndone\nexec /usr/bin/git \"$@\"\n",
    )?;
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700))?;
    config.tools.git = wrapper;
    let cancel = CancellationToken::new();
    let git_tool = Git {
        config: &config,
        archive: archive.clone(),
        work: tempfile::Builder::new()
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(dir.path())?,
        cancel: &cancel,
    };
    let index = dir.path().join("index");
    fs::write(&index, &original_index)?;
    git_tool.put_index(&index, &BTreeMap::new()).await?;
    assert!(!dir.path().join("git-wrapper.calls").exists());
    let entries: BTreeMap<_, _> = (0..129)
        .map(|n| {
            (
                format!("{PREFIX}/revisions/test {n:03},file.txt"),
                oid.clone(),
            )
        })
        .collect();
    git_tool.put_index(&index, &entries).await?;
    let expected = entries
        .iter()
        .map(|(path, oid)| format!("100644 {oid} 0\t{path}"))
        .chain([unrelated])
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(
        git_tool.raw(&["ls-files", "--stage"], Some(&index)).await?,
        expected
    );
    let tree = git_tool.raw(&["write-tree"], Some(&index)).await?;
    assert_eq!(
        git_tool
            .raw(&["ls-tree", "-r", &tree], None)
            .await?
            .lines()
            .count(),
        130
    );
    assert_eq!(fs::read(archive.join(".git/index"))?, original_index);
    assert_eq!(
        fs::read_to_string(dir.path().join("git-wrapper.calls"))?
            .lines()
            .count(),
        3,
        "129 entries must use three bounded Git index updates"
    );
    Ok(())
}
#[tokio::test]
async fn deterministic_abc_exact_views_and_unrelated_index() -> Result<()> {
    let (_dir, store, config) = fixture()?;
    let archive = config.archive.directory.as_ref().unwrap();
    fs::create_dir(archive)?;
    git(archive, &["init", "-b", "main"])?;
    fs::write(archive.join("unrelated.txt"), "staged private note")?;
    git(archive, &["add", "unrelated.txt"])?;
    let a = capture("100", "A唯一正文\r\nsecond\rthird");
    let mut b = capture("200", "B");
    b.relations.push(Related {
        target: a.identity.clone(),
        kind: RelationKind::Quotes,
    });
    let mut c = capture("300", "C");
    c.relations.push(Related {
        target: a.identity.clone(),
        kind: RelationKind::RepliesTo,
    });
    let a_id = store.capture(&a)?.unwrap();
    store.capture(&b)?;
    store.capture(&c)?;
    let mut missing = capture("400", "unavailable ancestor");
    missing.content = None;
    missing.status = FetchStatus::Missing;
    store.capture(&missing)?;
    let mut d = capture("500", "D links missing");
    d.relations.push(Related {
        target: missing.identity.clone(),
        kind: RelationKind::Quotes,
    });
    store.capture(&d)?;
    let views = [
        store.create_view(
            &b.identity,
            ViewPolicy {
                max_nodes: 10,
                max_depth: 4,
            },
        )?,
        store.create_view(
            &d.identity,
            ViewPolicy {
                max_nodes: 10,
                max_depth: 4,
            },
        )?,
    ];
    let cancel = CancellationToken::new();
    let status = store.project(&config, 100, &cancel).await?;
    ensure!(status.event_id > 0 && status.pushed_commit.is_none());
    assert_eq!(git(archive, &["rev-list", "--count", "HEAD"])?, "7");
    assert_eq!(git(archive, &["ls-tree", "--name-only", "HEAD"])?, PREFIX);
    assert_eq!(
        git(archive, &["diff", "--cached", "--name-only"])?,
        "unrelated.txt"
    );
    let files = snapshot(archive)?;
    assert_eq!(
        files
            .values()
            .filter(|v| v.starts_with("A唯一正文".as_bytes()))
            .count(),
        1
    );
    assert!(!files[&format!("{PREFIX}/revisions/{a_id}/body.md")].contains(&b'\r'));
    let meta: Value =
        serde_json::from_slice(&files[&format!("{PREFIX}/revisions/{a_id}/metadata.json")])?;
    assert_eq!(meta["original_newline_map"].as_array().unwrap().len(), 2);
    for view in views {
        let exported: View =
            serde_json::from_slice(&files[&format!("{PREFIX}/views/{}.json", view.id()?)])?;
        assert_eq!(view, exported);
        for member in view.members {
            if let Some(rev) = member.revision_id {
                assert!(files.contains_key(&format!("{PREFIX}/revisions/{rev}/body.md")));
            } else {
                assert!(member.missing_reason.is_some());
            }
        }
    }
    let head = git(archive, &["rev-parse", "HEAD"])?;
    store.project(&config, 100, &cancel).await?;
    assert_eq!(head, git(archive, &["rev-parse", "HEAD"])?);
    assert_eq!(files, snapshot(archive)?);
    Ok(())
}
#[test]
fn projection_crash_child() -> Result<()> {
    let Ok(path) = std::env::var("ARCHIVE_PROJECTION_FIXTURE") else {
        return Ok(());
    };
    let config: Config = serde_json::from_slice(&fs::read(path)?)?;
    let store = Store::open(&config.state_dir)?;
    tokio::runtime::Runtime::new()?.block_on(store.project(
        &config,
        1,
        &CancellationToken::new(),
    ))?;
    Ok(())
}
#[tokio::test]
async fn process_death_after_write_and_commit_reconciles_once() -> Result<()> {
    for point in ["after-write", "after-commit"] {
        let (dir, store, config) = fixture()?;
        store.capture(&capture("100", "Crash recovery source"))?;
        let config_path = dir.path().join("fixture.json");
        fs::write(&config_path, serde_json::to_vec(&config)?)?;
        let exit = tokio::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "projection::tests::projection_crash_child",
                "--nocapture",
            ])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("ARCHIVE_PROJECTION_FIXTURE", config_path)
            .env("ARCHIVE_PROJECTION_CRASH", point)
            .kill_on_drop(true)
            .output()
            .await?;
        assert_eq!(
            exit.status.code(),
            Some(94),
            "{}",
            String::from_utf8_lossy(&exit.stderr)
        );
        assert_eq!(
            store
                .projection_status(config.archive.directory.as_ref().unwrap())?
                .event_id,
            0
        );
        assert_eq!(
            store
                .connection()?
                .query_row("SELECT count(*) FROM projection_intents", [], |r| r
                    .get::<_, i64>(0))?,
            1
        );
        store
            .project(&config, 10, &CancellationToken::new())
            .await?;
        assert_eq!(
            git(
                config.archive.directory.as_ref().unwrap(),
                &["rev-list", "--count", "HEAD"]
            )?,
            "1"
        );
        assert_eq!(
            git(
                config.archive.directory.as_ref().unwrap(),
                &["status", "--porcelain"]
            )?,
            ""
        );
    }
    Ok(())
}
#[tokio::test]
async fn rejected_and_lost_push_have_separate_local_remote_state() -> Result<()> {
    for lost in [false, true] {
        let (dir, store, mut config) = fixture()?;
        let bare = dir.path().join("remote.git");
        fs::create_dir(&bare)?;
        git(&bare, &["init", "--bare", "-b", "main"])?;
        if !lost {
            let hook = bare.join("hooks/pre-receive");
            fs::write(&hook, "#!/bin/sh\nexit 1\n")?;
            fs::set_permissions(hook, fs::Permissions::from_mode(0o700))?;
        }
        if lost {
            let wrapper = dir.path().join("git-wrapper");
            fs::write(
                &wrapper,
                "#!/bin/sh\nfor arg in \"$@\"; do\n if [ \"$arg\" = push ]; then\n  /usr/bin/git \"$@\" || exit $?\n  exit 73\n fi\ndone\nexec /usr/bin/git \"$@\"\n",
            )?;
            fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o700))?;
            config.tools.git = wrapper;
        }
        config.archive.remote = Some(bare.to_string_lossy().into());
        config.archive.push = true;
        store.capture(&capture("100", "Push source"))?;
        let result = store.project(&config, 10, &CancellationToken::new()).await;
        let status = store.projection_status(config.archive.directory.as_ref().unwrap())?;
        assert!(status.local_commit.is_some());
        let push: String =
            store
                .connection()?
                .query_row("SELECT state FROM projection_pushes", [], |r| r.get(0))?;
        if lost {
            result?;
            assert_eq!(status.local_commit, status.pushed_commit);
            assert_eq!(push, "confirmed");
        } else {
            assert!(result.is_err());
            assert!(status.pushed_commit.is_none());
            assert_eq!(push, "rejected");
            assert_eq!(store.source_counts()?.1, 1);
        }
    }
    Ok(())
}
#[tokio::test]
async fn public_code_targets_are_rejected_before_export() -> Result<()> {
    let (_dir, store, mut config) = fixture()?;
    store.capture(&capture("100", "Private text"))?;
    let archive = config.archive.directory.clone().unwrap();
    fs::create_dir(&archive)?;
    git(&archive, &["init", "-b", "main"])?;
    git(
        &archive,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/RyderFreeman4Logos/telegram-archive-bot.git",
        ],
    )?;
    assert!(
        store
            .project(&config, 10, &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(!archive.join(PREFIX).exists());
    config.archive.directory = Some(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    assert!(
        store
            .project(&config, 10, &CancellationToken::new())
            .await
            .is_err()
    );
    Ok(())
}
#[tokio::test]
async fn managed_external_edits_and_symlinks_are_preserved() -> Result<()> {
    let (_dir, store, config) = fixture()?;
    let source = capture("100", "One");
    store.capture(&source)?;
    store
        .project(&config, 10, &CancellationToken::new())
        .await?;
    let archive = config.archive.directory.as_ref().unwrap();
    let path = format!("{PREFIX}/sources/{}.json", source.identity.id());
    let original = fs::read(archive.join(&path))?;
    fs::write(archive.join(&path), "operator edit")?;
    store.capture(&capture("100", "Two"))?;
    assert!(
        store
            .project(&config, 10, &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(fs::read(archive.join(&path))?, b"operator edit");
    fs::write(archive.join(&path), &original)?;
    store
        .project(&config, 10, &CancellationToken::new())
        .await?;
    fs::write(archive.join(&path), "staged edit")?;
    git(archive, &["add", "--", &path])?;
    store.capture(&capture("100", "Three"))?;
    assert!(
        store
            .project(&config, 10, &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(git(archive, &["diff", "--cached", "--name-only"])?, path);
    fs::write(archive.join(&path), &original)?;
    git(archive, &["add", "--", &path])?;
    fs::remove_file(archive.join(&path))?;
    symlink("/etc/passwd", archive.join(&path))?;
    assert!(
        store
            .project(&config, 10, &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(
        archive
            .join(&path)
            .symlink_metadata()?
            .file_type()
            .is_symlink()
    );
    Ok(())
}
#[tokio::test]
async fn concurrent_projectors_and_git_outage_leave_canonical_state_usable() -> Result<()> {
    let (_dir, store, mut config) = fixture()?;
    let revision = store.capture(&capture("100", "Stable source"))?.unwrap();
    let real = config.tools.git.clone();
    config.tools.git = "/does/not/exist/git".into();
    assert!(
        store
            .project(&config, 10, &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(store.revision(&revision)?.content.body, "Stable source");
    config.tools.git = real;
    let cancel = CancellationToken::new();
    let (a, b) = tokio::join!(
        store.project(&config, 10, &cancel),
        store.project(&config, 10, &cancel)
    );
    a?;
    b?;
    assert_eq!(
        git(
            config.archive.directory.as_ref().unwrap(),
            &["rev-list", "--count", "HEAD"]
        )?,
        "1"
    );
    Ok(())
}

#[tokio::test]
async fn separate_stores_cannot_share_one_projection_destination() -> Result<()> {
    let (_one, first, config) = fixture()?;
    first.capture(&capture("100", "First archive"))?;
    first
        .project(&config, 10, &CancellationToken::new())
        .await?;
    let (_two, second, mut other) = fixture()?;
    second.capture(&capture("200", "Other private archive"))?;
    other.archive.directory = config.archive.directory.clone();
    assert!(
        second
            .project(&other, 10, &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(
        git(
            config.archive.directory.as_ref().unwrap(),
            &["rev-list", "--count", "HEAD"]
        )?,
        "1"
    );
    Ok(())
}
