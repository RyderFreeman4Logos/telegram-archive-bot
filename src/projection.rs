//! A semantic projection; SQLite and retained CAS objects remain authoritative.
use crate::{
    config::Config,
    digest,
    process::Tool,
    storage::{Store, now, sync_dir},
};
use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Component, Path, PathBuf},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const PREFIX: &str = "archive-v1";
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Event {
    id: i64,
    kind: String,
    entity: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Intent {
    events: Vec<Event>,
    files: BTreeMap<String, Vec<u8>>,
    base: Option<String>,
    branch: String,
    commit: Option<String>,
    created: i64,
    prior_index: BTreeMap<String, String>,
    lock: Option<(u64, u64, String, String)>,
}
impl Intent {
    fn last(&self) -> i64 {
        self.events.last().expect("nonempty event batch").id
    }
    fn marker(&self) -> Result<String> {
        Ok(format!(
            "Archive-Events: {}-{}\nArchive-Manifest: {}",
            self.events[0].id,
            self.last(),
            digest(serde_json::to_vec(&self.files)?)
        ))
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct ProjectionStatus {
    pub event_id: i64,
    pub local_commit: Option<String>,
    pub pushed_commit: Option<String>,
}
struct Git<'a> {
    config: &'a Config,
    archive: PathBuf,
    work: tempfile::TempDir,
    cancel: &'a CancellationToken,
}
impl Git<'_> {
    async fn raw(&self, args: &[&str], index: Option<&Path>) -> Result<String> {
        let mut environment = BTreeMap::<OsString, OsString>::from([
            ("GIT_TERMINAL_PROMPT".into(), "0".into()),
            ("GIT_CONFIG_NOSYSTEM".into(), "1".into()),
            ("GIT_CONFIG_GLOBAL".into(), "/dev/null".into()),
            ("GIT_ALLOW_PROTOCOL".into(), "https:http:ssh:file".into()),
            ("GIT_AUTHOR_NAME".into(), "Telegram Archive".into()),
            ("GIT_COMMITTER_NAME".into(), "Telegram Archive".into()),
            (
                "GIT_AUTHOR_EMAIL".into(),
                "telegram-archive@localhost".into(),
            ),
            (
                "GIT_COMMITTER_EMAIL".into(),
                "telegram-archive@localhost".into(),
            ),
        ]);
        if let Some(index) = index {
            environment.insert("GIT_INDEX_FILE".into(), index.as_os_str().to_owned());
        }
        let mut argv = vec![
            "-c".into(),
            "core.hooksPath=/dev/null".into(),
            "-c".into(),
            "core.fsmonitor=false".into(),
            "-c".into(),
            "credential.helper=".into(),
            "-C".into(),
            self.archive.as_os_str().to_owned(),
        ];
        argv.extend(args.iter().map(OsString::from));
        let output = Tool {
            executable: self.config.tools.git.clone(),
            args: argv,
            environment,
            directory: self.work.path().to_owned(),
            timeout: Duration::from_secs(self.config.limits.media_seconds),
            log_bytes: self.config.limits.log_bytes,
            stdout_bytes: Some(self.config.limits.input_bytes as usize),
            work_bytes: self.config.limits.work_bytes,
            secrets: vec![],
        }
        .run(self.cancel)
        .await?;
        Ok(output.stdout.trim_end_matches('\n').to_owned())
    }
    async fn head(&self) -> Result<Option<String>> {
        let branch = self.raw(&["symbolic-ref", "HEAD"], None).await?;
        let line = self
            .raw(&["for-each-ref", "--format=%(objectname)", &branch], None)
            .await?;
        Ok((!line.is_empty()).then_some(line))
    }
    async fn blob(&self, commit: Option<&str>, path: &str) -> Result<Option<String>> {
        let Some(commit) = commit else {
            return Ok(None);
        };
        let tree = self.raw(&["ls-tree", commit, "--", path], None).await?;
        if tree.is_empty() {
            return Ok(None);
        }
        let (entry, name) = tree.split_once('\t').context("invalid Git tree entry")?;
        let parts: Vec<_> = entry.split_whitespace().collect();
        ensure!(
            parts.len() == 3 && parts[0] == "100644" && parts[1] == "blob" && name == path,
            "managed path has non-file Git mode"
        );
        Ok(Some(parts[2].into()))
    }
    async fn stage(&self, path: &str, index: Option<&Path>) -> Result<String> {
        self.raw(&["ls-files", "--stage", "--", path], index).await
    }
    async fn put_index(&self, index: &Path, entries: &BTreeMap<String, String>) -> Result<()> {
        let mut entries = entries.iter();
        loop {
            let values: Vec<_> = entries
                .by_ref()
                .take(64)
                .map(|(path, oid)| format!("100644,{oid},{path}"))
                .collect();
            if values.is_empty() {
                break;
            }
            let mut args = vec!["update-index", "--add"];
            for value in &values {
                args.extend(["--cacheinfo", value]);
            }
            self.raw(&args, Some(index)).await?;
        }
        Ok(())
    }
}
fn safe_path(root: &Path, path: &str, create: bool) -> Result<PathBuf> {
    ensure!(
        path.starts_with("archive-v1/"),
        "projection path outside namespace"
    );
    let mut full = root.to_owned();
    let parts: Vec<_> = Path::new(path).components().collect();
    for (i, part) in parts.iter().enumerate() {
        ensure!(
            matches!(part, Component::Normal(_)),
            "unsafe projection path"
        );
        full.push(part);
        if let Ok(meta) = fs::symlink_metadata(&full) {
            ensure!(
                !meta.file_type().is_symlink(),
                "symlink in managed projection"
            );
            ensure!(
                if i + 1 == parts.len() {
                    meta.is_file() && meta.nlink() == 1
                } else {
                    meta.is_dir()
                },
                "unsafe projection file type"
            );
        } else if i + 1 < parts.len() && create {
            fs::create_dir(&full)?;
            fs::set_permissions(&full, fs::Permissions::from_mode(0o700))?;
            sync_dir(full.parent().unwrap())?;
        }
    }
    Ok(full)
}
fn json_bytes(value: &impl Serialize) -> Result<Vec<u8>> {
    let mut b = serde_json::to_vec_pretty(value)?;
    b.push(b'\n');
    Ok(b)
}
fn insert_json(
    files: &mut BTreeMap<String, Vec<u8>>,
    path: String,
    value: &impl Serialize,
) -> Result<()> {
    files.insert(path, json_bytes(value)?);
    Ok(())
}
fn identity(
    tx: &Transaction<'_>,
    files: &mut BTreeMap<String, Vec<u8>>,
    source: &str,
) -> Result<()> {
    let (provider, external): (String, String) = tx.query_row(
        "SELECT provider,external_id FROM sources WHERE id=?1",
        [source],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    ensure!(
        digest(serde_json::to_vec(&crate::sources::Identity {
            provider: provider.clone(),
            external_id: external.clone()
        })?) == source,
        "source identity digest mismatch"
    );
    insert_json(
        files,
        format!("{PREFIX}/sources/{source}.json"),
        &json!({"schema_version":1,"id":source,"provider":provider,"external_id":external}),
    )
}
fn revision(tx: &Transaction<'_>, files: &mut BTreeMap<String, Vec<u8>>, id: &str) -> Result<()> {
    let (source, hash, body, metadata): (String, String, String, String) = tx.query_row(
        "SELECT source_id,content_digest,body,metadata FROM source_revisions WHERE id=?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    let content = crate::sources::Content {
        body: body.clone(),
        metadata: serde_json::from_str(&metadata)?,
    };
    ensure!(
        digest(serde_json::to_vec(&content)?) == hash && digest(format!("{source}:{hash}")) == id,
        "revision integrity failure"
    );
    identity(tx, files, &source)?;
    let mut normalized = String::new();
    let mut newline_map = Vec::new();
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\r' {
            let paired = chars.peek() == Some(&'\n');
            if paired {
                chars.next();
            }
            newline_map.push((normalized.len(), paired));
            normalized.push('\n');
        } else {
            normalized.push(c);
        }
    }

    insert_json(
        files,
        format!("{PREFIX}/revisions/{id}/metadata.json"),
        &json!({"schema_version":1,"id":id,"source_id":source,"content_digest":hash,"body_file":"body.md","body_file_sha256":digest(&normalized),"original_newline_map":newline_map,"metadata":content.metadata}),
    )?;
    files.insert(
        format!("{PREFIX}/revisions/{id}/body.md"),
        normalized.into_bytes(),
    );
    Ok(())
}
impl Store {
    fn projection_save(&self, name: &str, intent: &Intent) -> Result<()> {
        self.connection()?.execute("INSERT INTO projection_intents VALUES(?1,?2) ON CONFLICT(name) DO UPDATE SET intent=excluded.intent",params![name,serde_json::to_string(intent)?])?;
        Ok(())
    }
    fn render_events(&self, after: i64) -> Result<Option<Intent>> {
        // Publication lock precedes the read snapshot, so CAS GC cannot remove a referenced object.
        let _publication = self.publication_lock()?;
        let mut conn = self.connection()?;
        let tx = conn.transaction()?;
        let first: Option<(i64, Option<String>)> = tx
            .query_row(
                "SELECT id,group_key FROM projection_events WHERE id>?1 ORDER BY id LIMIT 1",
                [after],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((first, group)) = first else {
            return Ok(None);
        };
        let events=tx.prepare("SELECT id,kind,entity_id FROM projection_events WHERE id=?1 OR (group_key=?2 AND id>?3) ORDER BY id")?.query_map(params![first,group,after],|r|Ok(Event{id:r.get(0)?,kind:r.get(1)?,entity:r.get(2)?}))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut files = BTreeMap::new();
        insert_json(
            &mut files,
            format!("{PREFIX}/schema.json"),
            &json!({"schema_version":1,"kind":"semantic-archive","body_newlines":"LF","binary_blobs_included":false,"operational_state_included":false}),
        )?;
        for event in &events {
            let id = &event.entity;
            match event.kind.as_str() {
                "revision" => revision(&tx, &mut files, id)?,
                "alias" => {
                    let alias: Vec<String> = serde_json::from_str(id)?;
                    ensure!(alias.len() == 3, "invalid alias");
                    identity(&tx, &mut files, &alias[0])?;
                    insert_json(
                        &mut files,
                        format!("{PREFIX}/aliases/{}.json", digest(id)),
                        &json!({"schema_version":1,"source_id":alias[0],"url":alias[1],"method":alias[2]}),
                    )?;
                }
                "observation" => {
                    let (source,rev,time,status,extractor,evidence,raw):(String,Option<String>,i64,String,String,String,Option<String>)=tx.query_row("SELECT source_id,revision_id,observed_at,status,extractor,evidence,raw_digest FROM observations WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
                    identity(&tx, &mut files, &source)?;
                    if let Some(rev) = &rev {
                        revision(&tx, &mut files, rev)?;
                    }
                    let edges=tx.prepare("SELECT target_id,kind FROM relations WHERE observation_id=?1 ORDER BY kind,target_id")?.query_map([id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
                    for (target, _) in &edges {
                        identity(&tx, &mut files, target)?;
                    }
                    insert_json(
                        &mut files,
                        format!("{PREFIX}/observations/{id}.json"),
                        &json!({"schema_version":1,"id":id.parse::<i64>()?,"source_id":source,"revision_id":rev,"observed_at":time,"status":status,"extractor":extractor,"evidence":serde_json::from_str::<Value>(&evidence)?,"raw_digest":raw,"relations":edges}),
                    )?;
                    if let Some(original)=tx.query_row("SELECT manifest FROM semantic_unavailable WHERE kind='observation' AND entity_id=?1",[id],|r|r.get::<_,String>(0)).optional()? {
                        insert_json(&mut files,format!("{PREFIX}/observations/{id}.json"),&serde_json::from_str::<Value>(&original)?)?;
                    }
                }
                "semantic_derivative" => {
                    self.blob_path(id)?;
                    let (manifest,text):(String,Option<String>)=tx.query_row("SELECT manifest,text_digest FROM semantic_derivatives WHERE derivation_key=?1",[id],|r|Ok((r.get(0)?,r.get(1)?)))?;
                    insert_json(
                        &mut files,
                        format!("{PREFIX}/derivatives/{id}/manifest.json"),
                        &serde_json::from_str::<Value>(&manifest)?,
                    )?;
                    if let Some(text) = text {
                        self.verify_blob(&text)?;
                        files.insert(
                            format!("{PREFIX}/derivatives/{id}/translation.md"),
                            fs::read(self.blob_path(&text)?)?,
                        );
                    }
                }
                "semantic_artifact" => {
                    self.blob_path(id)?;
                    let manifest:String=tx.query_row("SELECT manifest FROM semantic_unavailable WHERE kind='artifact' AND entity_id=?1",[id],|r|r.get(0))?;
                    insert_json(
                        &mut files,
                        format!("{PREFIX}/artifacts/{id}.json"),
                        &serde_json::from_str::<Value>(&manifest)?,
                    )?;
                }
                "view" => {
                    let manifest: String =
                        tx.query_row("SELECT manifest FROM views WHERE id=?1", [id], |r| r.get(0))?;
                    let view: crate::sources::View = serde_json::from_str(&manifest)?;
                    ensure!(view.id()? == *id, "view integrity failure");
                    for member in &view.members {
                        identity(&tx, &mut files, &member.source_id)?;
                        if let Some(rev) = &member.revision_id {
                            revision(&tx, &mut files, rev)?;
                        } else {
                            ensure!(
                                member.missing_reason.is_some(),
                                "view lacks missing-node reason"
                            );
                        }
                    }
                    insert_json(&mut files, format!("{PREFIX}/views/{id}.json"), &view)?;
                }
                "derivative" => {
                    self.blob_path(id)?;
                    let (kind,blob,input):(String,String,String)=tx.query_row("SELECT kind,manifest_digest,input_digest FROM derivatives WHERE derivation_key=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
                    let retained = self.verify_blob(&blob)?;
                    ensure!(
                        retained.size <= 64 * 1024 * 1024,
                        "derivative manifest too large"
                    );
                    let export_text = matches!(
                        kind.as_str(),
                        "metadata" | "metadata_chunk" | "translation" | "spoken_text"
                    );
                    let mut value = json!({"schema_version":1,"derivation_key":id,"kind":kind,"input_digest":input,"manifest_digest":blob,"manifest_exported":export_text});
                    if export_text {
                        let manifest: Value =
                            serde_json::from_slice(&fs::read(self.blob_path(&blob)?)?)?;
                        if kind == "translation" {
                            let output = manifest["output_digest"]
                                .as_str()
                                .context("translation has no output")?;
                            self.verify_blob(output)?;
                            let text = String::from_utf8(fs::read(self.blob_path(output)?)?)?;
                            files.insert(
                                format!("{PREFIX}/derivatives/{id}/translation.md"),
                                text.replace("\r\n", "\n").replace('\r', "\n").into_bytes(),
                            );
                        }
                        value["manifest"] = manifest;
                    }
                    insert_json(
                        &mut files,
                        format!("{PREFIX}/derivatives/{id}/manifest.json"),
                        &value,
                    )?;
                }
                "artifact" => {
                    self.blob_path(id)?;
                    let manifest: String =
                        tx.query_row("SELECT manifest FROM artifacts WHERE id=?1", [id], |r| {
                            r.get(0)
                        })?;
                    insert_json(
                        &mut files,
                        format!("{PREFIX}/artifacts/{id}.json"),
                        &serde_json::from_str::<Value>(&manifest)?,
                    )?;
                }
                _ => bail!("unknown projection event kind"),
            }
        }
        tx.commit()?;
        Ok(Some(Intent {
            events,
            files,
            base: None,
            branch: String::new(),
            commit: None,
            created: now(),
            prior_index: BTreeMap::new(),
            lock: None,
        }))
    }
}

struct IndexLock {
    path: PathBuf,
    file: File,
    dev: u64,
    ino: u64,
    renamed: bool,
}
impl IndexLock {
    fn new(index: &Path) -> Result<Self> {
        let path = index.with_file_name(format!(
            "{}.lock",
            index
                .file_name()
                .context("invalid index path")?
                .to_string_lossy()
        ));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&path)
            .context("archive index is locked; refusing to disturb another Git operation")?;
        let meta = file.metadata()?;
        Ok(Self {
            path,
            file,
            dev: meta.dev(),
            ino: meta.ino(),
            renamed: false,
        })
    }
}
impl Drop for IndexLock {
    fn drop(&mut self) {
        if !self.renamed
            && fs::symlink_metadata(&self.path)
                .is_ok_and(|m| m.dev() == self.dev && m.ino() == self.ino)
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl Store {
    pub fn projection_status(&self, directory: &Path) -> Result<ProjectionStatus> {
        let name = digest(directory.canonicalize()?.as_os_str().as_encoded_bytes());
        Ok(self
            .connection()?
            .query_row(
                "SELECT event_id,local_commit,pushed_commit FROM projection_cursors WHERE name=?1",
                [name],
                |r| {
                    Ok(ProjectionStatus {
                        event_id: r.get(0)?,
                        local_commit: r.get(1)?,
                        pushed_commit: r.get(2)?,
                    })
                },
            )
            .optional()?
            .unwrap_or(ProjectionStatus {
                event_id: 0,
                local_commit: None,
                pushed_commit: None,
            }))
    }
    /// Process at most `max_events` event batches, then optionally reconcile/push the local checkpoint.
    pub async fn project(
        &self,
        config: &Config,
        max_events: usize,
        cancel: &CancellationToken,
    ) -> Result<ProjectionStatus> {
        ensure!(
            !self.recovery_paused()?,
            "external side effects are paused after recovery"
        );
        ensure!(max_events > 0, "projection batch limit must be positive");
        let destination = config
            .archive
            .directory
            .as_ref()
            .context("archive directory is not configured")?;
        ensure!(
            destination.is_absolute(),
            "archive directory must be absolute"
        );
        if !destination.exists() {
            fs::create_dir(destination)?;
            fs::set_permissions(destination, fs::Permissions::from_mode(0o700))?;
        }
        let archive = destination.canonicalize()?;
        let code = Path::new(env!("CARGO_MANIFEST_DIR")).canonicalize().ok();
        ensure!(
            !archive.starts_with(self.root()) && !self.root().starts_with(&archive),
            "archive and state directories overlap"
        );
        ensure!(
            !code
                .as_ref()
                .is_some_and(|p| archive.starts_with(p) || p.starts_with(&archive)),
            "public code and archive directories overlap"
        );
        ensure!(
            config.state_dir.canonicalize()? == self.root(),
            "projection configuration refers to another store"
        );
        let _state_lock =
            projection_lock(&self.root().join("projection.lock"), config, cancel).await?;
        let git = Git {
            config,
            archive: archive.clone(),
            work: self.workspace("git-projection")?,
            cancel,
        };
        if !archive.join(".git").exists() {
            git.raw(
                &[
                    "init",
                    "--initial-branch",
                    config.archive.branch.as_deref().unwrap_or("main"),
                ],
                None,
            )
            .await?;
        }
        ensure!(
            Path::new(&git.raw(&["rev-parse", "--show-toplevel"], None).await?).canonicalize()?
                == archive,
            "archive must be its own non-bare Git worktree"
        );
        let common = PathBuf::from(
            git.raw(
                &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                None,
            )
            .await?,
        )
        .canonicalize()?;
        ensure!(
            !code.as_ref().is_some_and(|p| common.starts_with(p)),
            "archive Git metadata belongs to public code checkout"
        );
        let _archive_lock =
            projection_lock(&common.join("telegram-archive.lock"), config, cancel).await?;
        let store_id: String =
            self.connection()?
                .query_row("SELECT id FROM archive_identity", [], |r| r.get(0))?;
        let binding = common.join("telegram-archive-store");
        if !binding.exists() {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&binding)?;
            file.write_all(store_id.as_bytes())?;
            file.sync_all()?;
            sync_dir(&common)?;
        }
        ensure!(
            binding.symlink_metadata()?.is_file()
                && !binding.symlink_metadata()?.file_type().is_symlink()
                && binding.metadata()?.len() == 64
                && fs::read(&binding)? == store_id.as_bytes(),
            "archive is bound to another canonical store"
        );
        let urls = git.raw(&["remote", "-v"], None).await?;
        ensure!(
            !is_code_remote(&urls),
            "archive has the public application repository as a remote"
        );
        let branch = git.raw(&["symbolic-ref", "HEAD"], None).await?;
        if let Some(configured) = &config.archive.branch {
            git.raw(
                &["check-ref-format", &format!("refs/heads/{configured}")],
                None,
            )
            .await?;
            ensure!(
                branch == format!("refs/heads/{configured}"),
                "archive checked-out branch differs from configured branch"
            );
        }
        let name = digest(archive.as_os_str().as_encoded_bytes());
        for _ in 0..max_events {
            ensure!(!cancel.is_cancelled(), "projection cancelled");
            let saved: Option<String> = self
                .connection()?
                .query_row(
                    "SELECT intent FROM projection_intents WHERE name=?1",
                    [&name],
                    |r| r.get(0),
                )
                .optional()?;
            let mut intent = if let Some(saved) = saved {
                serde_json::from_str::<Intent>(&saved)?
            } else {
                let after = self.projection_status(&archive)?.event_id;
                let Some(mut intent) = self.render_events(after)? else {
                    break;
                };
                ensure!(
                    intent.files.values().map(|b| b.len() as u64).sum::<u64>()
                        <= config.limits.work_bytes,
                    "projection event exceeds work byte limit"
                );
                intent.base = git.head().await?;
                intent.branch = branch.clone();
                for path in intent.files.keys() {
                    let staged = git.stage(path, None).await?;
                    let expected = git
                        .blob(intent.base.as_deref(), path)
                        .await?
                        .map(|oid| format!("100644 {oid} 0\t{path}"))
                        .unwrap_or_default();
                    ensure!(
                        staged == expected,
                        "managed path contains unrelated staged edits"
                    );
                    intent.prior_index.insert(path.clone(), staged);
                }
                self.projection_save(&name, &intent)?;
                intent
            };
            ensure!(
                intent.branch == branch,
                "pending projection belongs to another branch"
            );
            self.apply_projection(&git, &name, &mut intent).await?;
        }
        let status = self.projection_status(&archive)?;
        if config.archive.push
            && let Some(commit) = &status.local_commit
        {
            self.push_projection(&git, &name, commit).await?;
        }
        self.projection_status(&archive)
    }
    async fn apply_projection(&self, git: &Git<'_>, name: &str, intent: &mut Intent) -> Result<()> {
        let head = git.head().await?;
        let committed = if let Some(commit) = &intent.commit {
            if head.as_ref() == Some(commit) {
                true
            } else if head != intent.base {
                git.raw(
                    &[
                        "merge-base",
                        "--is-ancestor",
                        commit,
                        head.as_deref().context("archive history disappeared")?,
                    ],
                    None,
                )
                .await?;
                true
            } else {
                false
            }
        } else {
            ensure!(
                head == intent.base,
                "archive HEAD changed during pending projection"
            );
            false
        };
        if committed && intent.commit != intent.base {
            let message = git
                .raw(
                    &[
                        "log",
                        "-1",
                        "--format=%B",
                        intent.commit.as_deref().context("missing event commit")?,
                    ],
                    None,
                )
                .await?;
            ensure!(
                message.contains(&intent.marker()?),
                "projection commit marker mismatch"
            );
        }
        let index = PathBuf::from(
            git.raw(
                &["rev-parse", "--path-format=absolute", "--git-path", "index"],
                None,
            )
            .await?,
        );
        let lock_path = index.with_file_name("index.lock");
        if let Some((dev, ino, initial, final_hash)) = &intent.lock {
            if let Ok(meta) = fs::symlink_metadata(&lock_path) {
                ensure!(
                    meta.is_file()
                        && !meta.file_type().is_symlink()
                        && meta.nlink() == 1
                        && meta.dev() == *dev
                        && meta.ino() == *ino,
                    "Git lock ownership changed; manual reconciliation required"
                );
                let hash = digest(fs::read(&lock_path)?);
                ensure!(
                    hash == *initial || hash == *final_hash,
                    "partial Git index lock requires manual reconciliation"
                );
                fs::remove_file(&lock_path)?;
                sync_dir(index.parent().unwrap())?;
            }
            intent.lock = None;
            self.projection_save(name, intent)?;
        }
        let mut entries = BTreeMap::new();
        for (path, bytes) in &intent.files {
            let full = safe_path(&git.archive, path, !committed)?;
            let file = git.work.path().join("rendered");
            fs::write(&file, bytes)?;
            let expected = git
                .raw(
                    &[
                        "hash-object",
                        "-w",
                        "--no-filters",
                        file.to_str().context("non-UTF8 workspace")?,
                    ],
                    None,
                )
                .await?;
            if committed {
                ensure!(
                    git.blob(intent.commit.as_deref(), path).await?.as_deref()
                        == Some(expected.as_str()),
                    "event commit does not contain expected file hashes"
                );
            } else {
                if full.exists() {
                    let actual = git
                        .raw(
                            &[
                                "hash-object",
                                "--no-filters",
                                full.to_str().context("non-UTF8 archive path")?,
                            ],
                            None,
                        )
                        .await?;
                    ensure!(
                        actual == expected
                            || git.blob(intent.base.as_deref(), path).await?.as_deref()
                                == Some(actual.as_str()),
                        "managed file has external worktree edits"
                    );
                } else {
                    ensure!(
                        git.blob(intent.base.as_deref(), path).await?.is_none(),
                        "managed file was externally deleted"
                    );
                }
                let mut tmp = tempfile::NamedTempFile::new_in(full.parent().unwrap())?;
                tmp.as_file()
                    .set_permissions(fs::Permissions::from_mode(0o600))?;
                tmp.write_all(bytes)?;
                tmp.as_file().sync_all()?;
                tmp.persist(&full)?;
                sync_dir(full.parent().unwrap())?;
            }
            entries.insert(path.clone(), expected);
        }
        #[cfg(test)]
        projection_failpoint("after-write");
        let mut index_lock = IndexLock::new(&index)?;
        let user_index = git.work.path().join("user-index");
        if index.exists() {
            ensure!(
                index.symlink_metadata()?.is_file()
                    && !index.symlink_metadata()?.file_type().is_symlink(),
                "unsafe Git index"
            );
            fs::copy(&index, &user_index)?;
        } else if let Some(head) = &head {
            git.raw(&["read-tree", head], Some(&user_index)).await?;
        } else {
            git.raw(&["read-tree", "--empty"], Some(&user_index))
                .await?;
        }
        for (path, oid) in &entries {
            let staged = git.stage(path, Some(&user_index)).await?;
            ensure!(
                staged == intent.prior_index[path] || staged == format!("100644 {oid} 0\t{path}"),
                "managed staging changed; refusing to overwrite"
            );
        }
        git.put_index(&user_index, &entries).await?;
        let final_bytes = fs::read(&user_index)?;
        intent.lock = Some((
            index_lock.dev,
            index_lock.ino,
            digest([]),
            digest(&final_bytes),
        ));
        self.projection_save(name, intent)?;
        if !committed {
            let commit_index = git.work.path().join("commit-index");
            if let Some(base) = &intent.base {
                git.raw(&["read-tree", base], Some(&commit_index)).await?;
            } else {
                git.raw(&["read-tree", "--empty"], Some(&commit_index))
                    .await?;
            }
            git.put_index(&commit_index, &entries).await?;
            let tree = git.raw(&["write-tree"], Some(&commit_index)).await?;
            let empty = if let Some(base) = &intent.base {
                git.raw(&["rev-parse", &format!("{base}^{{tree}}")], None)
                    .await?
                    == tree
            } else {
                false
            };
            if empty {
                intent.commit = intent.base.clone();
            } else if intent.commit.is_none() {
                let message = git.work.path().join("commit-message");
                fs::write(
                    &message,
                    format!(
                        "Archive knowledge event {}\n\n{}\n",
                        intent.last(),
                        intent.marker()?
                    ),
                )?;
                let mut args = vec!["commit-tree", tree.as_str()];
                if let Some(base) = &intent.base {
                    args.extend(["-p", base]);
                }
                args.extend(["-F", message.to_str().context("invalid message path")?]);
                intent.commit = Some(git.raw(&args, None).await?);
            }
            self.projection_save(name, intent)?;
            index_lock.file.write_all(&final_bytes)?;
            index_lock.file.sync_all()?;
            let unborn = "0".repeat(intent.commit.as_ref().context("missing commit")?.len());
            let expected = intent.base.as_deref().unwrap_or(&unborn);
            git.raw(
                &[
                    "update-ref",
                    &intent.branch,
                    intent
                        .commit
                        .as_deref()
                        .context("missing projection commit")?,
                    expected,
                ],
                None,
            )
            .await?;
        } else {
            index_lock.file.write_all(&final_bytes)?;
            index_lock.file.sync_all()?;
        }
        fs::rename(&index_lock.path, &index)?;
        index_lock.renamed = true;
        sync_dir(index.parent().unwrap())?;
        #[cfg(test)]
        projection_failpoint("after-commit");
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute("INSERT INTO projection_cursors(name,event_id,local_commit) VALUES(?1,?2,?3) ON CONFLICT(name) DO UPDATE SET event_id=excluded.event_id,local_commit=excluded.local_commit",params![name,intent.last(),intent.commit])?;
        tx.execute("DELETE FROM projection_intents WHERE name=?1", [name])?;
        tx.commit()?;
        Ok(())
    }
    async fn push_projection(&self, git: &Git<'_>, name: &str, commit: &str) -> Result<()> {
        let remote = git
            .config
            .archive
            .remote
            .as_deref()
            .context("push remote missing")?;
        let branch = git
            .config
            .archive
            .branch
            .as_deref()
            .context("push branch missing")?;
        ensure!(
            !remote.is_empty() && !remote.starts_with('-') && !remote.chars().any(char::is_control),
            "invalid archive remote"
        );
        let url = if remote.contains("://") || remote.contains('/') || remote.contains(':') {
            remote.to_owned()
        } else {
            git.raw(
                &["remote", "get-url", "--push", "--all", "--", remote],
                None,
            )
            .await?
        };
        ensure!(
            !url.is_empty() && !url.chars().any(char::is_control),
            "multiple or invalid push destinations"
        );
        ensure!(
            !is_code_remote(&url),
            "archive push targets public code repository"
        );
        if let Ok(url) = url::Url::parse(&url) {
            ensure!(
                url.password().is_none()
                    && (url.scheme() == "ssh" || url.username().is_empty())
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "embedded archive remote credentials are forbidden"
            );
        }
        let target = digest(serde_json::to_vec(&(remote, &url, branch))?);
        self.connection()?.execute("INSERT INTO projection_pushes VALUES(?1,?2,?3,'pending') ON CONFLICT(name,target) DO UPDATE SET commit_id=excluded.commit_id,state='pending'",params![name,target,commit])?;
        if self
            .remote_contains(git, &url, branch, commit)
            .await
            .unwrap_or(false)
        {
            return self.confirm_push(name, &target, commit);
        }
        let sent = git
            .raw(
                &["push", "--", &url, &format!("{commit}:refs/heads/{branch}")],
                None,
            )
            .await;
        match self.remote_contains(git, &url, branch, commit).await {
            Ok(true) => self.confirm_push(name, &target, commit),
            observed => {
                let state = if observed.is_ok() && sent.is_err() {
                    "rejected"
                } else {
                    "uncertain"
                };
                self.connection()?.execute(
                    "UPDATE projection_pushes SET state=?1 WHERE name=?2 AND target=?3",
                    params![state, name, target],
                )?;
                bail!("archive push {state}; local commit remains usable")
            }
        }
    }
    async fn remote_contains(
        &self,
        git: &Git<'_>,
        remote: &str,
        branch: &str,
        commit: &str,
    ) -> Result<bool> {
        let reference = format!("refs/heads/{branch}");
        let output = git
            .raw(&["ls-remote", "--heads", "--", remote, &reference], None)
            .await?;
        let Some((oid, found)) = output.split_once('\t') else {
            ensure!(output.is_empty(), "invalid remote ref response");
            return Ok(false);
        };
        ensure!(
            found == reference
                && matches!(oid.len(), 40 | 64)
                && oid.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid remote ref"
        );
        if oid == commit {
            return Ok(true);
        }
        git.raw(
            &[
                "fetch",
                "--no-tags",
                "--no-write-fetch-head",
                "--",
                remote,
                &reference,
            ],
            None,
        )
        .await?;
        Ok(git
            .raw(&["merge-base", "--is-ancestor", commit, oid], None)
            .await
            .is_ok())
    }
    fn confirm_push(&self, name: &str, target: &str, commit: &str) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE projection_pushes SET state='confirmed' WHERE name=?1 AND target=?2",
            params![name, target],
        )?;
        tx.execute(
            "UPDATE projection_cursors SET pushed_commit=?1 WHERE name=?2",
            params![commit, name],
        )?;
        tx.commit()?;
        Ok(())
    }
}
async fn projection_lock(path: &Path, config: &Config, cancel: &CancellationToken) -> Result<File> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let start = tokio::time::Instant::now();
    loop {
        match lock.try_lock_exclusive() {
            Ok(()) => return Ok(lock),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                ensure!(
                    start.elapsed() < Duration::from_secs(config.limits.media_seconds),
                    "projection lock timeout"
                );
                tokio::select! {_=cancel.cancelled()=>bail!("projection cancelled"),_=tokio::time::sleep(Duration::from_millis(25))=>{}}
            }
            Err(e) => return Err(e.into()),
        }
    }
}
fn is_code_remote(remote: &str) -> bool {
    let remote = remote.to_ascii_lowercase();
    remote.contains("github.com/ryderfreeman4logos/telegram-archive-bot")
        || remote.contains("github.com:ryderfreeman4logos/telegram-archive-bot")
}
#[cfg(test)]
fn projection_failpoint(point: &str) {
    if std::env::var("ARCHIVE_PROJECTION_CRASH").as_deref() == Ok(point) {
        std::process::exit(94);
    }
}

#[cfg(test)]
#[path = "projection_tests.rs"]
mod tests;
