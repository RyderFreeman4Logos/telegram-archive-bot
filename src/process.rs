use crate::storage::{Store, private_dir};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File},
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Component, Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolFailure {
    Missing,
    InvalidConfiguration,
    Timeout,
    Cancelled,
    LogLimit,
    WorkLimit,
    UnsafeOutput,
    Nonzero,
    Io,
}
impl std::fmt::Display for ToolFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tool failure: {self:?}")
    }
}
impl std::error::Error for ToolFailure {}

pub struct Tool {
    pub executable: PathBuf,
    pub args: Vec<OsString>,
    pub environment: BTreeMap<OsString, OsString>,
    pub directory: PathBuf,
    pub timeout: Duration,
    pub log_bytes: usize,
    pub stdout_bytes: Option<usize>,
    pub work_bytes: u64,
    pub secrets: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolOutput {
    pub executable: PathBuf,
    pub executable_digest: String,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
}

#[derive(Debug)]
pub struct ToolError {
    pub kind: ToolFailure,
    pub report: Option<Box<ToolOutput>>,
}
impl From<ToolFailure> for ToolError {
    fn from(kind: ToolFailure) -> Self {
        Self { kind, report: None }
    }
}
impl std::fmt::Display for ToolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)
    }
}
impl std::error::Error for ToolError {}
impl PartialEq<ToolFailure> for ToolError {
    fn eq(&self, other: &ToolFailure) -> bool {
        self.kind == *other
    }
}

impl Store {
    pub fn workspace(&self, job_id: &str) -> Result<tempfile::TempDir> {
        ensure!(
            !job_id.is_empty()
                && job_id.len() <= 80
                && job_id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'-'),
            "invalid workspace identity"
        );
        private_dir(&self.root().join("work"))?;
        Ok(tempfile::Builder::new()
            .prefix(&format!("{job_id}-"))
            .permissions(fs::Permissions::from_mode(0o700))
            .tempdir_in(self.root().join("work"))?)
    }
}

pub fn confined_file(root: &Path, reported: &Path) -> Result<PathBuf> {
    let root = root.canonicalize()?;
    let path = if reported.is_absolute() {
        reported.to_owned()
    } else {
        root.join(reported)
    };
    ensure!(
        !path.components().any(|c| matches!(c, Component::ParentDir)),
        "parent traversal in output path"
    );
    let relative = path.strip_prefix(&root)?;
    let mut current = root.clone();
    for component in relative.components() {
        ensure!(
            matches!(component, Component::Normal(_)),
            "invalid output component"
        );
        current.push(component.as_os_str());
        ensure!(
            !current.symlink_metadata()?.file_type().is_symlink(),
            "symlink in output path"
        );
    }
    let path = path.canonicalize()?;
    ensure!(
        path.starts_with(root) && path.is_file(),
        "output is not a confined regular file"
    );
    ensure!(
        path.metadata()?.nlink() == 1,
        "hard-linked tool output is forbidden"
    );
    Ok(path)
}

fn workspace_size(root: &Path, limit: u64) -> std::result::Result<(), ToolFailure> {
    let mut total = 0u64;
    let mut entries = 0u32;
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(dir).map_err(|_| ToolFailure::Io)? {
            let entry = entry.map_err(|_| ToolFailure::Io)?;
            let meta = entry
                .path()
                .symlink_metadata()
                .map_err(|_| ToolFailure::Io)?;
            entries += 1;
            if entries > 10000 {
                return Err(ToolFailure::WorkLimit);
            }
            if meta.file_type().is_symlink() {
                return Err(ToolFailure::UnsafeOutput);
            }
            if meta.is_dir() {
                dirs.push(entry.path());
            } else if meta.is_file() {
                total = total.saturating_add(meta.len());
                if total > limit {
                    return Err(ToolFailure::WorkLimit);
                }
            } else {
                return Err(ToolFailure::UnsafeOutput);
            }
        }
    }
    Ok(())
}

fn redact(mut text: String, secrets: &[String]) -> String {
    for secret in secrets.iter().filter(|s| !s.is_empty()) {
        text = text.replace(secret, "[REDACTED]");
        for length in (1..secret.len())
            .rev()
            .filter(|n| secret.is_char_boundary(*n))
        {
            if text.ends_with(&secret[..length]) {
                text.truncate(text.len() - length);
                text.push_str("[REDACTED]");
                break;
            }
        }
    }
    text
}

async fn capture(
    mut reader: impl AsyncRead + Unpin,
    limit: usize,
    overflow: Arc<AtomicBool>,
) -> std::io::Result<Vec<u8>> {
    let mut captured = vec![];
    let mut buf = [0u8; 8192];
    loop {
        let count = reader.read(&mut buf).await?;
        if count == 0 {
            return Ok(captured);
        }
        let keep = count.min(limit.saturating_sub(captured.len()));
        captured.extend_from_slice(&buf[..keep]);
        if keep < count {
            overflow.store(true, Ordering::SeqCst);
        }
    }
}

fn signal_group(group: i32, signal: i32) {
    unsafe {
        libc::kill(-group, signal);
    }
}

fn reap_group(group: i32) {
    for _ in 0..100 {
        let mut status = 0;
        let result = unsafe { libc::waitpid(-group, &mut status, libc::WNOHANG) };
        if result < 0 {
            break;
        }
        if result == 0 {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

struct GroupGuard(i32, bool);
impl Drop for GroupGuard {
    fn drop(&mut self) {
        if self.1 {
            signal_group(self.0, libc::SIGKILL);
            let group = self.0;
            std::thread::spawn(move || reap_group(group));
        }
    }
}

impl Tool {
    pub async fn run(
        &self,
        cancel: &CancellationToken,
    ) -> std::result::Result<ToolOutput, ToolError> {
        use sha2::{Digest, Sha256};
        if !self.executable.is_absolute()
            || self.timeout.is_zero()
            || self.log_bytes == 0
            || self.stdout_bytes == Some(0)
            || self.work_bytes == 0
        {
            return Err(ToolFailure::InvalidConfiguration.into());
        }
        private_dir(&self.directory).map_err(|_| ToolFailure::InvalidConfiguration)?;
        workspace_size(&self.directory, self.work_bytes)?;
        let executable = self
            .executable
            .canonicalize()
            .map_err(|_| ToolFailure::Missing)?;
        let meta = executable.metadata().map_err(|_| ToolFailure::Missing)?;
        if !meta.is_file() || meta.permissions().mode() & 0o111 == 0 {
            return Err(ToolFailure::Missing.into());
        }
        let mut hash = Sha256::new();
        std::io::copy(
            &mut File::open(&executable).map_err(|_| ToolFailure::Missing)?,
            &mut hash,
        )
        .map_err(|_| ToolFailure::Io)?;
        let executable_digest = format!("{:x}", hash.finalize());
        static SUBREAPER: OnceLock<bool> = OnceLock::new();
        if !*SUBREAPER
            .get_or_init(|| unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0 })
        {
            return Err(ToolFailure::InvalidConfiguration.into());
        }
        if cancel.is_cancelled() {
            return Err(ToolFailure::Cancelled.into());
        }
        let mut command = std::process::Command::new(&executable);
        command
            .args(&self.args)
            .current_dir(&self.directory)
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("LANG", "C.UTF-8")
            .env("HOME", &self.directory)
            .env("TMPDIR", &self.directory)
            .env("XDG_CACHE_HOME", self.directory.join("cache"))
            .envs(&self.environment)
            .process_group(0)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let file_limit = self.work_bytes.min(i64::MAX as u64) as libc::rlim_t;
        unsafe {
            command.pre_exec(move || {
                let limit = libc::rlimit {
                    rlim_cur: file_limit,
                    rlim_max: file_limit,
                };
                if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut command = Command::from(command);
        command.kill_on_drop(true);
        let mut child = command.spawn().map_err(|_| ToolFailure::Io)?;
        let group = child.id().ok_or(ToolFailure::Io)? as i32;
        let mut guard = GroupGuard(group, true);
        let overflow = Arc::new(AtomicBool::new(false));
        let stdout = tokio::spawn(capture(
            child.stdout.take().ok_or(ToolFailure::Io)?,
            self.stdout_bytes.unwrap_or(self.log_bytes),
            overflow.clone(),
        ));
        let stderr = tokio::spawn(capture(
            child.stderr.take().ok_or(ToolFailure::Io)?,
            self.log_bytes,
            overflow.clone(),
        ));
        let deadline = tokio::time::sleep(self.timeout);
        tokio::pin!(deadline);
        let mut check = tokio::time::interval(Duration::from_millis(50));
        let mut status = None;
        let mut failure = loop {
            tokio::select! {biased;
                _=cancel.cancelled()=>break Some(ToolFailure::Cancelled),
                _=&mut deadline=>break Some(ToolFailure::Timeout),
                result=child.wait()=>{status=Some(result.map_err(|_|ToolFailure::Io)?);break None;},
                _=check.tick()=>{
                    if overflow.load(Ordering::SeqCst) {break Some(ToolFailure::LogLimit);}
                    if let Err(error)=workspace_size(&self.directory,self.work_bytes) {break Some(error);}
                }
            }
        };
        signal_group(group, libc::SIGTERM);
        if status.is_none() {
            status = tokio::time::timeout(Duration::from_millis(200), child.wait())
                .await
                .ok()
                .and_then(|r| r.ok());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        signal_group(group, libc::SIGKILL);
        if status.is_none() {
            status = Some(child.wait().await.map_err(|_| ToolFailure::Io)?);
        }
        tokio::task::spawn_blocking(move || reap_group(group))
            .await
            .map_err(|_| ToolFailure::Io)?;
        guard.1 = false;
        let stdout = stdout
            .await
            .map_err(|_| ToolFailure::Io)?
            .map_err(|_| ToolFailure::Io)?;
        let stderr = stderr
            .await
            .map_err(|_| ToolFailure::Io)?
            .map_err(|_| ToolFailure::Io)?;
        if overflow.load(Ordering::SeqCst) {
            failure.get_or_insert(ToolFailure::LogLimit);
        }
        if let Err(error) = workspace_size(&self.directory, self.work_bytes) {
            failure.get_or_insert(error);
        }
        let status = status.ok_or(ToolFailure::Io)?;
        if !status.success() {
            failure.get_or_insert(ToolFailure::Nonzero);
        }
        use std::os::unix::process::ExitStatusExt;
        let output = ToolOutput {
            executable,
            executable_digest,
            stdout: redact(String::from_utf8_lossy(&stdout).into_owned(), &self.secrets),
            stderr: redact(String::from_utf8_lossy(&stderr).into_owned(), &self.secrets),
            exit_code: status.code(),
            signal: status.signal(),
        };
        if let Some(kind) = failure {
            return Err(ToolError {
                kind,
                report: Some(Box::new(output)),
            });
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn redaction_handles_chunk_join_and_truncated_secret_suffix() {
        let secret = "synthetic-secret".to_owned();
        assert_eq!(
            redact(
                "prefix synthetic-secret suffix".into(),
                std::slice::from_ref(&secret)
            ),
            "prefix [REDACTED] suffix"
        );
        assert_eq!(
            redact("prefix synthetic-sec".into(), &[secret]),
            "prefix [REDACTED]"
        );
    }
}
