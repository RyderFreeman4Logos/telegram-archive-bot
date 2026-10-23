use crate::{
    config::Synthesis,
    digest,
    jobs::{Failure, Lease},
    process::{Tool, ToolFailure, confined_file},
    storage::{Store, now},
};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpokenTurn {
    pub speaker: Option<String>,
    pub voice: String,
    pub text: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpokenText {
    pub schema_version: u32,
    pub revision: String,
    pub input_digest: String,
    pub input_kind: String,
    pub policy: String,
    pub settings: Synthesis,
    pub turns: Vec<SpokenTurn>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpeechSegment {
    pub order: usize,
    pub text_digest: String,
    pub blob_digest: String,
    pub subtitle_digest: Option<String>,
    pub producer_digest: String,
    // Producer timings are local to this segment; F14 must measure before offsetting them.
    pub timing_validated: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpeechCandidate {
    pub schema_version: u32,
    pub revision: String,
    pub spoken_digest: String,
    pub config_id: String,
    pub segments: Vec<SpeechSegment>,
    pub pending_finalization: bool,
    pub created_at: i64,
}
pub struct Md2Audio {
    pub store: Store,
}

fn turns(text: &str, settings: &Synthesis) -> Result<Vec<SpokenTurn>> {
    ensure!(
        !text.trim().is_empty() && !text.contains('\0'),
        Failure::InvalidInput
    );
    if settings.speakers.is_empty() {
        return Ok(vec![SpokenTurn {
            speaker: None,
            voice: settings.voice.clone(),
            text: text.into(),
        }]);
    }
    let mut result: Vec<SpokenTurn> = vec![];
    for line in text.lines() {
        let label = line
            .trim_start_matches([' ', '\t'])
            .strip_prefix("**")
            .and_then(|s| s.split_once("**"))
            .and_then(|(label, body)| {
                label
                    .trim_end()
                    .strip_suffix([':', '：'])
                    .map(|s| (s.trim(), body.trim_start_matches([' ', '\t'])))
            });
        if let Some((speaker, body)) = label {
            ensure!(!speaker.contains(['*', ':', '：']), Failure::InvalidInput);
            let voice = settings
                .speakers
                .get(speaker)
                .ok_or(Failure::InvalidInput)?;
            result.push(SpokenTurn {
                speaker: Some(speaker.into()),
                voice: voice.clone(),
                text: body.into(),
            });
        } else if let Some(turn) = result.last_mut() {
            turn.text.push('\n');
            turn.text.push_str(line);
        } else {
            ensure!(line.trim().is_empty(), Failure::InvalidInput);
        }
    }
    ensure!(!result.is_empty(), Failure::InvalidInput);
    for turn in &mut result {
        turn.text = turn.text.trim().into();
        ensure!(!turn.text.is_empty(), Failure::InvalidInput);
    }
    Ok(result)
}
fn write_private(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}
fn failure(error: crate::process::ToolError) -> anyhow::Error {
    match error.kind {
        ToolFailure::Cancelled => Failure::Cancelled,
        ToolFailure::Timeout => Failure::Timeout,
        ToolFailure::Missing => Failure::ToolMissing,
        ToolFailure::Nonzero => Failure::ToolExit,
        _ => Failure::InvalidOutput,
    }
    .into()
}
impl Md2Audio {
    /// `input_digest` is a canonical body or a distinct persisted translation, never a display name.
    pub async fn synthesize(
        &self,
        lease: &Lease,
        revision: &str,
        input_digest: &str,
        input_kind: &str,
        cancel: &CancellationToken,
    ) -> Result<String> {
        self.store.heartbeat(lease)?;
        lease.config.validate()?;
        ensure!(
            lease.config.synthesis.enabled
                && matches!(input_kind, "original" | "translation" | "summary"),
            Failure::InvalidInput
        );
        let source = self.store.revision(revision)?;
        let input = self
            .store
            .read_blob(input_digest, lease.config.limits.input_bytes)?;
        if input_kind == "original" {
            ensure!(
                input == source.content.body.as_bytes(),
                Failure::InvalidInput
            );
        }
        let text = std::str::from_utf8(&input)
            .map_err(|_| Failure::InvalidInput)?
            .replace("\r\n", "\n")
            .replace('\r', "\n");
        let spoken = SpokenText {
            schema_version: 1,
            revision: revision.into(),
            input_digest: input_digest.into(),
            input_kind: input_kind.into(),
            policy: "verbatim-markdown-v1-labels-are-metadata".into(),
            settings: lease.config.synthesis.clone(),
            turns: turns(&text, &lease.config.synthesis)?,
        };
        ensure!(
            spoken.turns.len() <= lease.config.limits.graph_nodes,
            Failure::InvalidInput
        );
        let spoken_key = digest(serde_json::to_vec(
            &json!({"version":"spoken-v1","spoken":spoken,"config":lease.job.config_id,"tools":lease.tools}),
        )?);
        // Persist the exact producer text and resolved mapping before the first TTS request.
        let spoken_digest = self.store.publish_derivative(
            lease,
            &spoken_key,
            "spoken_text",
            input_digest,
            &spoken,
        )?;
        let seconds = lease.config.limits.synthesis_seconds;
        ensure!(
            lease
                .job
                .deadline
                .is_some_and(|d| d - now() >= seconds as i64),
            Failure::Timeout
        );
        let deadline = Instant::now() + Duration::from_secs(seconds);
        let mut segments = vec![];
        for (order, turn) in spoken.turns.iter().enumerate() {
            self.store.heartbeat(lease)?;
            let workspace = self.store.workspace(&lease.job.id)?;
            let input = workspace.path().join("input.md");
            let output = workspace.path().join("raw.mp3");
            let body = if let Some(speaker) = &turn.speaker {
                format!("**{speaker}:** {}", turn.text)
            } else {
                turn.text.clone()
            };
            write_private(&input, body.as_bytes())?;
            let mut args = vec![
                "synth".into(),
                input.into_os_string(),
                "--output".into(),
                output.clone().into_os_string(),
                "--no-strip".into(),
                "--speed".into(),
                spoken.settings.speed.to_string().into(),
            ];
            if let Some(speaker) = &turn.speaker {
                let config = workspace.path().join("speakers.toml");
                let map = BTreeMap::from([(
                    "speakers",
                    BTreeMap::from([(
                        speaker.clone(),
                        BTreeMap::from([("voice", turn.voice.clone())]),
                    )]),
                )]);
                write_private(&config, toml::to_string(&map)?.as_bytes())?;
                args.extend([
                    "--multi-speaker".into(),
                    "--speaker-config".into(),
                    config.into_os_string(),
                    "--no-cache".into(),
                ]);
            } else {
                args.extend(["--voice".into(), turn.voice.clone().into()]);
            }
            if spoken.settings.local {
                args.extend([
                    "--local-tts".into(),
                    "--local-model".into(),
                    spoken.settings.local_model.clone().into(),
                    "--volume".into(),
                    spoken.settings.volume.clone().into(),
                    "--threads".into(),
                    spoken.settings.threads.to_string().into(),
                ]);
            }
            if spoken.settings.subtitles {
                args.push("--srt".into());
            }
            let mut environment = BTreeMap::new();
            if spoken.settings.local {
                crate::storage::private_dir(&self.store.root().join("cache"))?;
                let data = self.store.root().join("cache/md2audio-data");
                crate::storage::private_dir(&data)?;
                environment.insert("XDG_DATA_HOME".into(), data.into_os_string());
            }
            // md2audio resolves ffmpeg on PATH; bind a private shim to the snapshotted absolute tool.
            let bin = workspace.path().join("bin");
            fs::create_dir(&bin)?;
            fs::set_permissions(&bin, fs::Permissions::from_mode(0o700))?;
            fs::copy(&lease.config.tools.ffmpeg, bin.join("ffmpeg"))?;
            fs::set_permissions(bin.join("ffmpeg"), fs::Permissions::from_mode(0o500))?;
            environment.insert(
                "PATH".into(),
                format!("{}:/usr/bin:/bin", bin.display()).into(),
            );
            let budget = deadline.saturating_duration_since(Instant::now());
            ensure!(!budget.is_zero(), Failure::Timeout);
            let tool = Tool {
                executable: lease.config.tools.md2audio.clone(),
                args,
                environment,
                directory: workspace.path().into(),
                timeout: budget,
                log_bytes: lease.config.limits.log_bytes,
                stdout_bytes: None,
                work_bytes: lease.config.limits.work_bytes,
                secrets: vec![],
            };
            let report = match tool.run(cancel).await {
                Ok(report) => report,
                Err(error) => {
                    if let Some(report) = &error.report {
                        let diagnostic = json!({"job":lease.job.id,"spoken":spoken_digest,"segment":order,"failure":format!("{:?}",error.kind),"report":report});
                        let key = digest(serde_json::to_vec(&diagnostic)?);
                        self.store.publish_derivative(
                            lease,
                            &key,
                            "producer_diagnostic",
                            input_digest,
                            &diagnostic,
                        )?;
                    }
                    return Err(failure(error));
                }
            };
            self.store.heartbeat(lease)?;
            let path =
                confined_file(workspace.path(), &output).map_err(|_| Failure::InvalidOutput)?;
            ensure!(path.metadata()?.len() > 0, Failure::InvalidOutput);
            let blob = self
                .store
                .publish_reader(File::open(path)?, lease.config.limits.work_bytes)?;
            let subtitle_digest = if spoken.settings.subtitles {
                let path = confined_file(workspace.path(), &output.with_extension("srt"))
                    .map_err(|_| Failure::InvalidOutput)?;
                Some(
                    self.store
                        .publish_reader(File::open(path)?, lease.config.limits.input_bytes)?
                        .digest,
                )
            } else {
                None
            };
            segments.push(SpeechSegment {
                order,
                text_digest: self.store.publish_bytes(turn.text.as_bytes())?.digest,
                blob_digest: blob.digest,
                subtitle_digest,
                producer_digest: report.executable_digest,
                timing_validated: false,
            });
        }
        let candidate = SpeechCandidate {
            schema_version: 1,
            revision: revision.into(),
            spoken_digest,
            config_id: lease.job.config_id.clone(),
            segments,
            pending_finalization: true,
            created_at: now(),
        };
        // Raw output is deliberately absent from the ready-artifact catalog.
        let key = digest(serde_json::to_vec(
            &json!({"version":"speech-candidate-v1","spoken":spoken_key,"segments":candidate.segments}),
        )?);
        self.store
            .publish_derivative(lease, &key, "speech_candidate", input_digest, &candidate)
    }
}
