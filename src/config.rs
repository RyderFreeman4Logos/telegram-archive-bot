use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "kind",
    content = "name",
    rename_all = "snake_case"
)]
pub enum SecretRef {
    Env(String),
    File(PathBuf),
}

impl SecretRef {
    pub fn read(&self) -> Result<String> {
        let value = match self {
            Self::Env(name) => std::env::var(name)
                .map_err(|_| anyhow::anyhow!("required secret environment variable is absent"))?,
            Self::File(path) => {
                use std::os::unix::fs::PermissionsExt;
                let metadata =
                    std::fs::symlink_metadata(path).context("secret file unavailable")?;
                ensure!(
                    metadata.is_file() && metadata.permissions().mode() & 0o077 == 0,
                    "secret file must be a regular file with mode 0600 or stricter"
                );
                ensure!(metadata.len() <= 16384, "secret file exceeds size limit");
                std::fs::read_to_string(path).context("cannot read secret file")?
            }
        };
        ensure!(!value.trim().is_empty(), "required secret is empty");
        Ok(value.trim().to_owned())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub schema_version: u32,
    pub state_dir: PathBuf,
    pub telegram: Telegram,
    pub model: Model,
    pub translation: Translation,
    pub synthesis: Synthesis,
    pub tools: Tools,
    pub limits: Limits,
    pub archive: Archive,
    pub sources: Sources,
    pub notion: Notion,
}

impl Default for Config {
    fn default() -> Self {
        let base = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".local/state")
            });
        Self {
            schema_version: 1,
            state_dir: base.join("telegram-archive-bot"),
            telegram: Telegram::default(),
            model: Model::default(),
            translation: Translation::default(),
            synthesis: Synthesis::default(),
            tools: Tools::default(),
            limits: Limits::default(),
            archive: Archive::default(),
            sources: Sources::default(),
            notion: Notion::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Telegram {
    pub enabled: bool,
    pub bot_id: i64,
    pub token: Option<SecretRef>,
    pub allowed_chats: Vec<i64>,
    pub allowed_users: Vec<i64>,
    pub narration_mode: AudioProfile,
}
impl Default for Telegram {
    fn default() -> Self {
        Self {
            enabled: false,
            bot_id: 0,
            token: None,
            allowed_chats: vec![],
            allowed_users: vec![],
            narration_mode: AudioProfile::VoiceNote,
        }
    }
}
impl Telegram {
    pub fn authorized(&self, chat: i64, user: i64) -> bool {
        self.allowed_chats.contains(&chat) && self.allowed_users.contains(&user)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioProfile {
    VoiceNote,
    AudioTrack,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Model {
    pub enabled: bool,
    pub endpoint: String,
    pub name: String,
    pub deployment_revision: String,
    pub key: Option<SecretRef>,
    pub input_tokens: usize,
    pub output_tokens: usize,
    pub language: String,
    pub prompt_version: String,
}
impl Default for Model {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://127.0.0.1:8080/v1".into(),
            name: "configured-model".into(),
            deployment_revision: "unconfigured".into(),
            key: None,
            input_tokens: 4096,
            output_tokens: 2048,
            language: "zh".into(),
            prompt_version: "metadata-v1".into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Translation {
    pub enabled: bool,
    pub endpoint: String,
    pub model: String,
    pub deployment_revision: String,
    pub profile: String,
    pub backend: String,
    pub key: Option<SecretRef>,
    pub language: String,
    pub context_tokens: u32,
}
impl Default for Translation {
    fn default() -> Self {
        Self {
            enabled: false,
            endpoint: "http://127.0.0.1:8080/v1".into(),
            model: "configured-model".into(),
            deployment_revision: "unconfigured".into(),
            profile: "generic".into(),
            backend: "openai_compatible".into(),
            key: None,
            language: "zh".into(),
            context_tokens: 8192,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Synthesis {
    pub enabled: bool,
    pub deployment_revision: String,
    pub local: bool,
    pub local_model: String,
    pub voice: String,
    pub speed: f64,
    pub language: String,
    pub speakers: BTreeMap<String, String>,
    pub seed: u64,
    pub threads: u32,
    pub volume: String,
    pub pitch: i32,
    pub subtitles: bool,
}
impl Default for Synthesis {
    fn default() -> Self {
        Self {
            enabled: false,
            deployment_revision: String::new(),
            local: false,
            local_model: "melo".into(),
            voice: "zh-CN-YunyangNeural".into(),
            speed: 1.0,
            language: "zh".into(),
            speakers: BTreeMap::new(),
            seed: 0,
            threads: 2,
            volume: "2.00".into(),
            pitch: 0,
            subtitles: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Tools {
    pub ffmpeg: PathBuf,
    pub ffprobe: PathBuf,
    pub yt_dlp: PathBuf,
    pub md2audio: PathBuf,
    pub pdfinfo: PathBuf,
    pub pdftotext: PathBuf,
    pub git: PathBuf,
    pub hymt_bridge: PathBuf,
    pub bubblewrap: PathBuf,
    pub browser: PathBuf,
}
impl Default for Tools {
    fn default() -> Self {
        Self {
            ffmpeg: "/usr/bin/ffmpeg".into(),
            ffprobe: "/usr/bin/ffprobe".into(),
            yt_dlp: "/usr/local/bin/yt-dlp".into(),
            md2audio: "/usr/local/bin/md2audio".into(),
            pdfinfo: "/usr/bin/pdfinfo".into(),
            pdftotext: "/usr/bin/pdftotext".into(),
            git: "/usr/bin/git".into(),
            hymt_bridge: "/usr/local/bin/archive-hymt-bridge".into(),
            bubblewrap: "/usr/bin/bwrap".into(),
            browser: "/usr/bin/chromium".into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub input_bytes: u64,
    pub media_bytes: u64,
    pub work_bytes: u64,
    pub semantic_files: usize,
    pub log_bytes: usize,
    pub download_seconds: u64,
    pub llm_seconds: u64,
    pub synthesis_seconds: u64,
    pub media_seconds: u64,
    pub send_seconds: u64,
    pub job_seconds: u64,
    pub max_attempts: u32,
    pub lease_seconds: u64,
    pub shutdown_seconds: u64,
    pub downloads: usize,
    pub models: usize,
    pub syntheses: usize,
    pub finalizers: usize,
    pub deliveries: usize,
    pub graph_nodes: usize,
    pub graph_depth: usize,
    pub reply_ancestors: usize,
    pub max_urls: usize,
    pub max_audio_seconds: u64,
    pub gc_grace_seconds: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            input_bytes: 10_000_000,
            media_bytes: 49_000_000,
            work_bytes: 1_000_000_000,
            semantic_files: 100_000,
            log_bytes: 65536,
            download_seconds: 300,
            llm_seconds: 1700,
            synthesis_seconds: 1800,
            media_seconds: 900,
            send_seconds: 120,
            job_seconds: 21600,
            max_attempts: 3,
            lease_seconds: 60,
            shutdown_seconds: 15,
            downloads: 2,
            models: 1,
            syntheses: 1,
            finalizers: 1,
            deliveries: 2,
            graph_nodes: 100,
            graph_depth: 16,
            reply_ancestors: 25,
            max_urls: 8,
            max_audio_seconds: 7200,
            gc_grace_seconds: 86400,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Archive {
    pub directory: Option<PathBuf>,
    pub remote: Option<String>,
    pub branch: Option<String>,
    pub push: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Sources {
    pub x_bearer: Option<SecretRef>,
    pub x_enabled: bool,
    pub x_fallback_enabled: bool,
    pub x_guest_enabled: bool,
    pub x_guest_bearer: Option<SecretRef>,
    pub media_enabled: bool,
    pub browser_enabled: bool,
}

/// DA05 (#72): references only; missing publication prerequisites are nonfatal.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Notion {
    pub enabled: bool,
    pub executable: Option<PathBuf>,
    pub token: Option<SecretRef>,
    pub destination: Option<NotionDestination>,
    pub request_seconds: u64,
    pub operation_seconds: u64,
    pub payload_bytes: usize,
    pub response_bytes: usize,
    pub record_bytes: usize,
    pub max_blocks: usize,
    pub max_requests: usize,
    /// Durable retry policy consumed by DA06; this adapter never retries writes.
    pub max_attempts: u32,
    pub retry_seconds: u64,
}

impl Default for Notion {
    fn default() -> Self {
        Self {
            enabled: true,
            executable: None,
            token: None,
            destination: None,
            request_seconds: 30,
            operation_seconds: 300,
            payload_bytes: 450_000,
            response_bytes: 2_000_000,
            record_bytes: 10_000_000,
            max_blocks: 10_000,
            max_requests: 256,
            max_attempts: 3,
            retry_seconds: 30,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NotionDestination {
    Page { id: String },
    DataSource { id: String, title_property: String },
}

impl Config {
    pub fn default_path() -> PathBuf {
        std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".config")
            })
            .join("telegram-archive-bot/config.toml")
    }
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read_to_string(path).context("cannot read configuration")?;
        // Do not echo a TOML source line: it may contain an accidentally pasted secret.
        let config: Self = toml::from_str(&bytes).map_err(|_| {
            anyhow::anyhow!("invalid configuration: check TOML syntax, field names and types")
        })?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(self.schema_version == 1, "unsupported configuration schema");
        ensure!(self.state_dir.is_absolute(), "state_dir must be absolute");
        self.notion.validate()?;
        ensure!(
            !(self.synthesis.local && !self.synthesis.speakers.is_empty()),
            "local TTS cannot use multiple speakers"
        );
        ensure!(
            matches!(self.synthesis.local_model.as_str(), "melo" | "aishell3"),
            "unsupported local TTS model"
        );
        ensure!(
            self.synthesis.speed.is_finite() && (0.5..=2.0).contains(&self.synthesis.speed),
            "speech speed must be between 0.5 and 2"
        );
        ensure!(
            self.synthesis.threads > 0,
            "synthesis threads must be positive"
        );
        ensure!(
            self.synthesis.pitch == 0,
            "md2audio does not expose pitch control"
        );
        let volume = &self.synthesis.volume;
        ensure!(
            volume.len() <= 8
                && volume.split('.').count() <= 2
                && volume
                    .split('.')
                    .next()
                    .is_some_and(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
                && volume.split('.').nth(1).is_none_or(|v| !v.is_empty()
                    && v.len() <= 2
                    && v.bytes().all(|b| b.is_ascii_digit()))
                && volume
                    .parse::<f64>()
                    .is_ok_and(|v| v.is_finite() && (0.0..=10.0).contains(&v)),
            "invalid local volume"
        );
        ensure!(
            !self.synthesis.voice.trim().is_empty() && !self.synthesis.language.trim().is_empty(),
            "empty synthesis voice or language"
        );
        if self.synthesis.local && self.synthesis.enabled {
            let speaker: usize = self
                .synthesis
                .voice
                .parse()
                .map_err(|_| anyhow::anyhow!("local voice must be a numeric speaker ID"))?;
            ensure!(
                speaker
                    < if self.synthesis.local_model == "aishell3" {
                        174
                    } else {
                        1
                    },
                "local speaker ID out of range"
            );
        }
        for (speaker, voice) in &self.synthesis.speakers {
            ensure!(
                !speaker.trim().is_empty()
                    && speaker == speaker.trim()
                    && !speaker.contains(['*', ':', '：', '\n', '\r'])
                    && !voice.trim().is_empty(),
                "invalid speaker mapping"
            );
        }
        for endpoint in [&self.model.endpoint, &self.translation.endpoint] {
            let url = url::Url::parse(endpoint)
                .map_err(|_| anyhow::anyhow!("invalid configured provider endpoint"))?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "provider endpoint must be HTTP(S) without credentials, query or fragment"
            );
        }
        if self.model.enabled {
            ensure!(
                self.model.deployment_revision != "unconfigured" && !self.model.name.is_empty(),
                "model requires name and deployment_revision"
            );
            ensure!(
                self.model.output_tokens <= i32::MAX as usize
                    && self
                        .model
                        .output_tokens
                        .checked_add(768)
                        .is_some_and(|reserved| self.model.input_tokens > reserved),
                "model context must reserve output, framing, and source bytes"
            );
        }
        if self.translation.enabled {
            ensure!(
                self.translation.deployment_revision != "unconfigured",
                "translation requires deployment_revision"
            );
            ensure!(
                matches!(
                    self.translation.backend.as_str(),
                    "openai_compatible" | "llama_cpp" | "vllm"
                ) && matches!(
                    self.translation.profile.as_str(),
                    "generic" | "hy_mt2_7b" | "hy_mt2_30b_a3b"
                ) && self.translation.context_tokens >= 2048
                    && !self.translation.model.trim().is_empty()
                    && !self.translation.language.trim().is_empty(),
                "invalid translation backend, profile or budget"
            );
        }
        ensure!(
            self.model.input_tokens >= 256 && self.model.output_tokens > 0,
            "model token budgets are too small"
        );
        if self.telegram.enabled {
            ensure!(
                self.telegram.bot_id > 0
                    && self.telegram.token.is_some()
                    && !self.telegram.allowed_chats.is_empty()
                    && !self.telegram.allowed_users.is_empty(),
                "Telegram requires bot_id, token reference, and explicit chat/user allowlists"
            );
        }
        if self.sources.x_enabled {
            ensure!(
                self.sources.x_bearer.is_some(),
                "X requires a bearer secret reference"
            );
        }
        if self.sources.x_guest_enabled {
            ensure!(
                self.sources.x_guest_bearer.is_some(),
                "guest article enrichment requires a public web-client bearer reference"
            );
            ensure!(
                self.sources.x_enabled || self.sources.x_fallback_enabled,
                "guest article enrichment requires an article-detecting provider"
            );
        }
        let l = &self.limits;
        ensure!(
            l.input_bytes > 0
                && l.media_bytes > 0
                && l.work_bytes >= l.media_bytes
                && l.log_bytes > 0,
            "invalid byte limits"
        );
        ensure!(
            [
                l.download_seconds,
                l.llm_seconds,
                l.synthesis_seconds,
                l.media_seconds,
                l.send_seconds,
                l.job_seconds,
                l.lease_seconds,
                l.shutdown_seconds
            ]
            .iter()
            .all(|v| *v > 0),
            "timeouts must be positive"
        );
        ensure!(
            [
                l.downloads,
                l.models,
                l.syntheses,
                l.finalizers,
                l.deliveries,
                l.graph_nodes,
                l.graph_depth,
                l.max_urls,
                l.semantic_files
            ]
            .iter()
            .all(|v| *v > 0)
                && l.max_attempts > 0,
            "concurrency, graph, URL, semantic file and attempt limits must be positive"
        );
        for path in [
            &self.tools.ffmpeg,
            &self.tools.ffprobe,
            &self.tools.yt_dlp,
            &self.tools.md2audio,
            &self.tools.pdfinfo,
            &self.tools.pdftotext,
            &self.tools.git,
            &self.tools.hymt_bridge,
            &self.tools.bubblewrap,
            &self.tools.browser,
        ] {
            ensure!(path.is_absolute(), "tool paths must be absolute");
        }
        if let Some(path) = &self.archive.directory {
            ensure!(
                path.is_absolute() && path != &self.state_dir,
                "archive requires a distinct absolute directory"
            );
        }
        ensure!(
            !self.archive.push
                || (self.archive.directory.is_some()
                    && self.archive.remote.is_some()
                    && self.archive.branch.is_some()),
            "push requires archive directory, remote and branch"
        );
        Ok(())
    }
    pub fn check_secrets(&self) -> Result<()> {
        for secret in [
            &self.telegram.token,
            &self.model.key,
            &self.translation.key,
            &self.sources.x_bearer,
            &self.sources.x_guest_bearer,
        ]
        .into_iter()
        .flatten()
        {
            secret.read()?;
        }
        Ok(())
    }
    pub fn fingerprint(&self) -> Result<String> {
        Ok(crate::digest(serde_json::to_vec(self)?))
    }
}

impl Notion {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.executable.as_ref().is_none_or(|p| p.is_absolute()),
            "Notion executable must be an absolute path"
        );
        ensure!(
            self.request_seconds > 0
                && self.request_seconds <= self.operation_seconds
                && self.operation_seconds <= 3600
                && (16_384..=500_000).contains(&self.payload_bytes)
                && (16_384..=10_000_000).contains(&self.response_bytes)
                && (1..=100_000_000).contains(&self.record_bytes)
                && (1..=100_000).contains(&self.max_blocks)
                && (1..=10_000).contains(&self.max_requests)
                && (1..=20).contains(&self.max_attempts)
                && (1..=86400).contains(&self.retry_seconds),
            "invalid Notion request, payload, operation or retry bounds"
        );
        Ok(())
    }
}

pub struct ConfigManager {
    current: Arc<Config>,
}
impl ConfigManager {
    pub fn new(config: Config) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            current: Arc::new(config),
        })
    }
    pub fn snapshot(&self) -> Arc<Config> {
        self.current.clone()
    }
    pub fn reload(&mut self, path: &Path) -> Result<()> {
        let next = Config::load(path)?;
        self.activate(next)
    }
    pub(crate) fn activate(&mut self, next: Config) -> Result<()> {
        next.validate()?;
        ensure!(
            next.state_dir == self.current.state_dir
                && next.telegram.bot_id == self.current.telegram.bot_id,
            "state_dir and bot_id changes require restart"
        );
        next.check_secrets()?;
        self.current = Arc::new(next);
        Ok(())
    }
}
