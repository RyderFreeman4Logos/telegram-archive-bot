use crate::{
    digest,
    jobs::{Failure, Lease, fence},
    process::{Tool, ToolFailure, ToolOutput, confined_file},
    storage::{Store, now},
};
use anyhow::{Result, ensure};
use rusqlite::{TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File},
    path::Path,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    AudioTrack,
    VoiceNote,
}
impl Profile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AudioTrack => "audio_track",
            Self::VoiceNote => "voice_note",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Repair {
    Reject,
    Remux,
    Transcode,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AudioInput {
    pub blob_digest: String,
    pub parent_id: String,
    pub producer_tools: Value,
    pub order: usize,
    pub subtitle_digest: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolVersion {
    pub sha256: String,
    pub version: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Validation {
    pub validator: String,
    pub full_decode: bool,
    pub duration_consistent: bool,
    pub probe_duration_ms: u64,
    pub measured_duration_ms: u64,
    pub tolerance_ms: u64,
    pub source_inspection_digest: String,
    pub original_completeness_verified: bool,
    pub source_had_errors: bool,
    pub tools: BTreeMap<String, ToolVersion>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Artifact {
    pub schema_version: u32,
    pub artifact_id: String,
    pub derivation_key: String,
    pub input_id: String,
    pub parent_id: String,
    pub producer_tools: Value,
    pub raw_digest: String,
    pub blob_digest: String,
    pub byte_length: u64,
    pub requested_profile: Profile,
    pub effective_profile: Profile,
    pub container: String,
    pub codec: String,
    pub mime: String,
    pub channels: u32,
    pub sample_rate: u32,
    pub duration_ms: u64,
    pub order: usize,
    pub timeline_offset_ms: u64,
    pub subtitle_digest: Option<String>,
    pub operation: String,
    pub validation_status: String,
    pub validation: Validation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Probe {
    container: String,
    codec: String,
    channels: u32,
    sample_rate: u32,
    duration_ms: Option<u64>,
    bit_rate: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Inspection {
    probe: Probe,
    decoded_ms: Option<u64>,
    consistent: bool,
    decode_report: Option<ToolOutput>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cue {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
}
fn timestamp(s: &str) -> Result<u64> {
    let pieces: Vec<_> = s.split([':', ',']).collect();
    ensure!(
        pieces.len() == 4
            && pieces[0].len() >= 2
            && pieces[0].len() <= 6
            && pieces[1].len() == 2
            && pieces[2].len() == 2
            && pieces[3].len() == 3
            && pieces.iter().all(|s| s.bytes().all(|b| b.is_ascii_digit())),
        Failure::InvalidOutput
    );
    let h: u64 = pieces[0].parse()?;
    let m: u64 = pieces[1].parse()?;
    let s: u64 = pieces[2].parse()?;
    let ms: u64 = pieces[3].parse()?;
    ensure!(m < 60 && s < 60, Failure::InvalidOutput);
    Ok(((h * 60 + m) * 60 + s) * 1000 + ms)
}
pub fn subtitle_cues(text: &str, duration_ms: u64, offset: u64) -> Result<Vec<Cue>> {
    let text = text.replace("\r\n", "\n");
    let mut cues = vec![];
    let mut previous = 0;
    for block in text.trim().split("\n\n") {
        let mut lines = block.lines();
        let index: usize = lines
            .next()
            .ok_or(Failure::InvalidOutput)?
            .trim()
            .parse()
            .map_err(|_| Failure::InvalidOutput)?;
        ensure!(index == cues.len() + 1, Failure::InvalidOutput);
        let (start, end) = lines
            .next()
            .and_then(|s| s.split_once(" --> "))
            .ok_or(Failure::InvalidOutput)?;
        let start = timestamp(start)?;
        let end = timestamp(end)?;
        let body = lines.collect::<Vec<_>>().join("\n");
        ensure!(
            start >= previous
                && end > start
                && end <= duration_ms.saturating_add(50)
                && !body.trim().is_empty(),
            Failure::InvalidOutput
        );
        previous = end;
        cues.push(Cue {
            start_ms: start.checked_add(offset).ok_or(Failure::InvalidOutput)?,
            end_ms: end.checked_add(offset).ok_or(Failure::InvalidOutput)?,
            text: body,
        });
    }
    ensure!(!cues.is_empty(), Failure::InvalidOutput);
    Ok(cues)
}
fn srt_time(ms: u64) -> String {
    format!(
        "{:02}:{:02}:{:02},{:03}",
        ms / 3_600_000,
        (ms / 60_000) % 60,
        (ms / 1000) % 60,
        ms % 1000
    )
}
fn srt(cues: &[Cue]) -> String {
    cues.iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "{}\n{} --> {}\n{}\n\n",
                i + 1,
                srt_time(c.start_ms),
                srt_time(c.end_ms),
                c.text
            )
        })
        .collect()
}
fn tolerance(ms: u64) -> u64 {
    (ms / 100).clamp(250, 1000)
}
fn tool_failure(error: crate::process::ToolError) -> anyhow::Error {
    match error.kind {
        ToolFailure::Cancelled => Failure::Cancelled,
        ToolFailure::Timeout => Failure::Timeout,
        ToolFailure::Missing => Failure::ToolMissing,
        ToolFailure::Nonzero => Failure::ToolExit,
        _ => Failure::InvalidOutput,
    }
    .into()
}
fn milliseconds(value: &Value) -> Option<u64> {
    let seconds = value.as_str()?.parse::<f64>().ok()?;
    (seconds.is_finite() && seconds > 0.0 && seconds < 1e9)
        .then_some((seconds * 1000.0).round() as u64)
}
fn probe(value: &Value) -> Result<Probe> {
    let streams = value["streams"].as_array().ok_or(Failure::InvalidOutput)?;
    ensure!(
        streams.len() == 1 && streams[0]["codec_type"] == "audio",
        Failure::InvalidOutput
    );
    let stream = &streams[0];
    let p = Probe {
        container: value["format"]["format_name"]
            .as_str()
            .ok_or(Failure::InvalidOutput)?
            .into(),
        codec: stream["codec_name"]
            .as_str()
            .ok_or(Failure::InvalidOutput)?
            .into(),
        channels: stream["channels"]
            .as_u64()
            .and_then(|v| v.try_into().ok())
            .ok_or(Failure::InvalidOutput)?,
        sample_rate: stream["sample_rate"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or(Failure::InvalidOutput)?,
        duration_ms: milliseconds(&value["format"]["duration"])
            .or_else(|| milliseconds(&stream["duration"])),
        bit_rate: stream["bit_rate"]
            .as_str()
            .or_else(|| value["format"]["bit_rate"].as_str())
            .and_then(|s| s.parse().ok()),
    };
    ensure!(
        p.channels > 0 && p.channels <= 8 && (8000..=384000).contains(&p.sample_rate),
        Failure::InvalidOutput
    );
    Ok(p)
}
fn compatible(p: &Probe, profile: Profile) -> bool {
    p.channels <= 2
        && match profile {
            Profile::AudioTrack => {
                (p.codec == "mp3" && p.container == "mp3")
                    || (p.codec == "aac" && p.container.split(',').any(|f| f == "mov"))
            }
            Profile::VoiceNote => p.codec == "opus" && p.container == "ogg",
        }
}
fn codec_compatible(p: &Probe, profile: Profile) -> bool {
    match profile {
        Profile::AudioTrack => matches!(p.codec.as_str(), "mp3" | "aac"),
        Profile::VoiceNote => p.codec == "opus",
    }
}
fn mux(p: &Probe, profile: Profile) -> (&'static str, &'static str, &'static str) {
    match profile {
        Profile::VoiceNote => ("ogg", "ogg", "audio/ogg"),
        Profile::AudioTrack if p.codec == "aac" => ("m4a", "mp4", "audio/mp4"),
        _ => ("mp3", "mp3", "audio/mpeg"),
    }
}

pub struct AudioFinalizer {
    pub store: Store,
}
struct Session<'a> {
    lease: &'a Lease,
    store: &'a Store,
    deadline: Instant,
    cancel: &'a CancellationToken,
}
impl Session<'_> {
    fn tool(&self, executable: &Path, args: Vec<OsString>, directory: &Path) -> Result<Tool> {
        let timeout = self.deadline.saturating_duration_since(Instant::now());
        ensure!(!timeout.is_zero(), Failure::Timeout);
        Ok(Tool {
            executable: executable.into(),
            args,
            environment: BTreeMap::new(),
            directory: directory.into(),
            timeout,
            log_bytes: self.lease.config.limits.log_bytes,
            stdout_bytes: None,
            work_bytes: self.lease.config.limits.work_bytes,
            secrets: vec![],
        })
    }
    async fn run(
        &self,
        executable: &Path,
        args: Vec<OsString>,
        directory: &Path,
    ) -> Result<ToolOutput> {
        self.store.heartbeat(self.lease)?;
        self.tool(executable, args, directory)?
            .run(self.cancel)
            .await
            .map_err(tool_failure)
    }
    async fn inspect(&self, path: &Path, directory: &Path) -> Result<Inspection> {
        let args=vec!["-v".into(),"error".into(),"-protocol_whitelist".into(),"file".into(),"-format_whitelist".into(),"mp3,mov,ogg,flac,wav,aac,matroska,webm".into(),"-show_entries".into(),"stream=codec_type,codec_name,channels,sample_rate,duration,bit_rate:format=format_name,duration,bit_rate".into(),"-of".into(),"json".into(),path.as_os_str().into()];
        let report = self
            .run(&self.lease.config.tools.ffprobe, args, directory)
            .await?;
        let probe =
            probe(&serde_json::from_str(&report.stdout).map_err(|_| Failure::InvalidOutput)?)?;
        let pcm = directory.join("timeline.pcm");
        let args = [
            "-hide_banner",
            "-nostdin",
            "-v",
            "error",
            "-xerror",
            "-err_detect",
            "explode",
            "-protocol_whitelist",
            "file",
            "-format_whitelist",
            "mp3,mov,ogg,flac,wav,aac,matroska,webm",
            "-i",
        ]
        .into_iter()
        .map(Into::into)
        .chain([path.as_os_str().into()])
        .chain(
            [
                "-map",
                "0:a:0",
                "-vn",
                "-sn",
                "-dn",
                "-ac",
                "1",
                "-ar",
                "48000",
                "-c:a",
                "pcm_s16le",
                "-f",
                "s16le",
                "-y",
            ]
            .into_iter()
            .map(Into::into),
        )
        .chain([pcm.clone().into_os_string()])
        .collect();
        let decoded = self
            .tool(&self.lease.config.tools.ffmpeg, args, directory)?
            .run(self.cancel)
            .await;
        let (decoded_ms, decode_report) = match decoded {
            Ok(report) => {
                let pcm = confined_file(directory, &pcm)?;
                let bytes = pcm.metadata()?.len();
                ensure!(bytes > 0 && bytes % 2 == 0, Failure::InvalidOutput);
                (Some(bytes / 96), Some(report))
            }
            Err(error) if error.kind == ToolFailure::Nonzero => (None, error.report.map(|r| *r)),
            Err(error) => return Err(tool_failure(error)),
        };
        if pcm.exists() {
            fs::remove_file(pcm)?;
        }
        let consistent = decoded_ms
            .zip(probe.duration_ms)
            .is_some_and(|(d, p)| d > 0 && d.abs_diff(p) <= tolerance(d));
        Ok(Inspection {
            probe,
            decoded_ms,
            consistent,
            decode_report,
        })
    }
    async fn convert(
        &self,
        input: &Path,
        output: &Path,
        p: &Probe,
        profile: Profile,
        transcode: bool,
        directory: &Path,
    ) -> Result<()> {
        let mut args: Vec<OsString> = [
            "-hide_banner",
            "-nostdin",
            "-v",
            "warning",
            "-protocol_whitelist",
            "file",
            "-format_whitelist",
            "mp3,mov,ogg,flac,wav,aac,matroska,webm",
            "-i",
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        args.push(input.as_os_str().into());
        args.extend(
            ["-map", "0:a:0", "-vn", "-sn", "-dn", "-map_metadata", "-1"]
                .into_iter()
                .map(Into::into),
        );
        if transcode {
            args.extend(
                match profile {
                    Profile::AudioTrack => vec![
                        "-c:a",
                        "libmp3lame",
                        "-b:a",
                        "96k",
                        "-ar",
                        "48000",
                        "-ac",
                        "2",
                    ],
                    Profile::VoiceNote => vec![
                        "-c:a", "libopus", "-b:a", "48k", "-vbr", "on", "-ar", "48000", "-ac", "1",
                    ],
                }
                .into_iter()
                .map(Into::into),
            );
        } else {
            args.extend(["-c:a".into(), "copy".into()]);
        }
        let format = if transcode {
            if profile == Profile::AudioTrack {
                "mp3"
            } else {
                "ogg"
            }
        } else {
            mux(p, profile).1
        };
        if format == "mp3" {
            args.extend(
                ["-write_xing", "1", "-id3v2_version", "3"]
                    .into_iter()
                    .map(Into::into),
            );
        }
        if format == "mp4" {
            args.extend(["-movflags", "+faststart"].into_iter().map(Into::into));
        }
        args.extend([
            "-f".into(),
            format.into(),
            "-n".into(),
            output.as_os_str().into(),
        ]);
        self.run(&self.lease.config.tools.ffmpeg, args, directory)
            .await?;
        Ok(())
    }
}
impl AudioFinalizer {
    pub async fn finalize(
        &self,
        lease: &Lease,
        input_id: &str,
        inputs: &[AudioInput],
        profile: Profile,
        repair: Repair,
        cancel: &CancellationToken,
    ) -> Result<Vec<Artifact>> {
        self.store.heartbeat(lease)?;
        ensure!(
            !inputs.is_empty() && inputs.len() <= lease.config.limits.graph_nodes,
            Failure::InvalidInput
        );
        ensure!(
            inputs.iter().enumerate().all(|(i, v)| v.order == i),
            Failure::InvalidInput
        );
        let seconds = lease.config.limits.media_seconds;
        ensure!(
            lease
                .job
                .deadline
                .is_some_and(|d| d - now() >= seconds as i64),
            Failure::Timeout
        );
        let session = Session {
            lease,
            store: &self.store,
            deadline: Instant::now() + Duration::from_secs(seconds),
            cancel,
        };
        let workspace = self.store.workspace(&lease.job.id)?;
        let root = workspace.path();
        let mut versions = BTreeMap::new();
        for (name, exe) in [
            ("ffmpeg", &lease.config.tools.ffmpeg),
            ("ffprobe", &lease.config.tools.ffprobe),
        ] {
            let r = session.run(exe, vec!["-version".into()], root).await?;
            versions.insert(
                name.into(),
                ToolVersion {
                    sha256: r.executable_digest,
                    version: r
                        .stdout
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .chars()
                        .take(512)
                        .collect(),
                },
            );
        }
        let mut output = vec![];
        let mut offset = 0u64;
        for input in inputs {
            self.store.verify_blob(&input.blob_digest)?;
            let raw = self.store.blob_path(&input.blob_digest)?;
            let source = session.inspect(&raw, root).await?;
            let source_digest = self
                .store
                .publish_bytes(&serde_json::to_vec(&source)?)?
                .digest;
            let source_valid = source.consistent && source.decoded_ms.is_some();
            let mut selected = raw.clone();
            let mut final_inspection = source.clone();
            let mut operation = "preserve";
            if !source_valid || !compatible(&source.probe, profile) {
                ensure!(repair != Repair::Reject, Failure::InvalidOutput);
                let remux = root.join(format!(
                    "remux-{}.{}",
                    input.order,
                    mux(&source.probe, profile).0
                ));
                if codec_compatible(&source.probe, profile) && source.decoded_ms.is_some() {
                    match session
                        .convert(&raw, &remux, &source.probe, profile, false, root)
                        .await
                    {
                        Ok(()) => {
                            final_inspection = session.inspect(&remux, root).await?;
                            selected = remux;
                            operation = "remux";
                        }
                        Err(error)
                            if repair == Repair::Transcode
                                && error.downcast_ref::<Failure>() == Some(&Failure::ToolExit) =>
                        {
                            final_inspection.consistent = false;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if !final_inspection.consistent || !compatible(&final_inspection.probe, profile) {
                    ensure!(repair == Repair::Transcode, Failure::InvalidOutput);
                    let transcode = root.join(format!(
                        "encoded-{}.{}",
                        input.order,
                        if profile == Profile::VoiceNote {
                            "ogg"
                        } else {
                            "mp3"
                        }
                    ));
                    session
                        .convert(&raw, &transcode, &source.probe, profile, true, root)
                        .await?;
                    final_inspection = session.inspect(&transcode, root).await?;
                    selected = transcode;
                    operation = "transcode";
                }
            }
            ensure!(
                final_inspection.consistent && compatible(&final_inspection.probe, profile),
                Failure::InvalidOutput
            );
            let duration = final_inspection.decoded_ms.ok_or(Failure::InvalidOutput)?;
            ensure!(
                duration > 0
                    && duration <= lease.config.limits.max_audio_seconds.saturating_mul(1000),
                Failure::InvalidOutput
            );
            let bytes = selected.metadata()?.len();
            if bytes == 0 || bytes > lease.config.limits.media_bytes {
                return Err(anyhow::Error::new(Failure::InvalidOutput).context(
                    "final audio exceeds the configured byte limit; split into ordered segments",
                ));
            }
            let floor = if profile == Profile::AudioTrack {
                64_000
            } else {
                16_000
            };
            ensure!(
                final_inspection
                    .probe
                    .bit_rate
                    .unwrap_or(bytes.saturating_mul(8000) / duration)
                    >= floor,
                Failure::InvalidOutput
            );
            let blob = if selected == raw {
                self.store.verify_blob(&input.blob_digest)?
            } else {
                self.store
                    .publish_reader(File::open(&selected)?, lease.config.limits.media_bytes)?
            };
            let subtitle_digest = if let Some(subtitle) = &input.subtitle_digest {
                let text = self
                    .store
                    .read_blob(subtitle, lease.config.limits.input_bytes)?;
                let cues = subtitle_cues(
                    std::str::from_utf8(&text).map_err(|_| Failure::InvalidOutput)?,
                    duration,
                    offset,
                )?;
                Some(self.store.publish_bytes(srt(&cues).as_bytes())?.digest)
            } else {
                None
            };
            let key = digest(serde_json::to_vec(
                &json!({"version":"audio-finalizer-v1","config":lease.job.config_id,"input":input_id,"parent":input,"profile":profile,"repair":repair,"tools":versions,"blob":blob.digest,"timeline":offset}),
            )?);
            let p = &final_inspection.probe;
            output.push(Artifact {
                schema_version: 1,
                artifact_id: digest(format!("artifact:{key}")),
                derivation_key: key,
                input_id: input_id.into(),
                parent_id: input.parent_id.clone(),
                producer_tools: input.producer_tools.clone(),
                raw_digest: input.blob_digest.clone(),
                blob_digest: blob.digest,
                byte_length: bytes,
                requested_profile: profile,
                effective_profile: profile,
                container: p.container.clone(),
                codec: p.codec.clone(),
                mime: mux(p, profile).2.into(),
                channels: p.channels,
                sample_rate: p.sample_rate,
                duration_ms: duration,
                order: input.order,
                timeline_offset_ms: offset,
                subtitle_digest,
                operation: operation.into(),
                validation_status: "passed".into(),
                validation: Validation {
                    validator: "pcm48k-mono-s16-v1".into(),
                    full_decode: true,
                    duration_consistent: true,
                    probe_duration_ms: p.duration_ms.unwrap(),
                    measured_duration_ms: duration,
                    tolerance_ms: tolerance(duration),
                    source_inspection_digest: source_digest,
                    original_completeness_verified: false,
                    source_had_errors: !source_valid,
                    tools: versions.clone(),
                },
            });
            offset = offset.checked_add(duration).ok_or(Failure::InvalidOutput)?;
            if selected != raw {
                fs::remove_file(selected)?;
            }
        }
        ensure!(!cancel.is_cancelled(), Failure::Cancelled);
        let mut conn = self.store.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        fence(&tx, lease, now())?;
        for artifact in &output {
            tx.execute(
                "INSERT INTO artifacts VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT DO NOTHING",
                params![
                    artifact.artifact_id,
                    input_id,
                    profile.as_str(),
                    artifact.blob_digest,
                    serde_json::to_string(&artifact.validation)?,
                    serde_json::to_string(artifact)?
                ],
            )?;
            tx.execute("INSERT INTO projection_events(kind,entity_id) VALUES('artifact',?1) ON CONFLICT DO NOTHING",[&artifact.artifact_id])?;
        }
        tx.commit()?;
        for artifact in &output {
            crate::operator::log(
                "artifact_validated",
                Some(lease),
                serde_json::json!({"artifact_id":artifact.artifact_id,"blob_digest":artifact.blob_digest,"duration_ms":artifact.duration_ms}),
            );
        }
        output
            .iter()
            .map(|a| self.store.artifact(&a.artifact_id))
            .collect()
    }
}
impl Store {
    pub fn artifact(&self, id: &str) -> Result<Artifact> {
        let (input,profile,blob,validation,manifest):(String,String,String,String,String)=self.connection()?.query_row("SELECT input_id,profile,blob_digest,validation,manifest FROM artifacts WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?;
        let a: Artifact = serde_json::from_str(&manifest)?;
        ensure!(
            a.schema_version == 1
                && a.artifact_id == id
                && a.input_id == input
                && a.effective_profile.as_str() == profile
                && a.blob_digest == blob
                && serde_json::to_value(&a.validation)?
                    == serde_json::from_str::<Value>(&validation)?
                && a.validation_status == "passed"
                && a.validation.full_decode
                && a.validation.duration_consistent
                && a.duration_ms == a.validation.measured_duration_ms
                && a.byte_length == self.verify_blob(&blob)?.size,
            Failure::InvalidOutput
        );
        Ok(a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_and_nonfinite_duration_is_not_validated_metadata() {
        let mut value = json!({"streams":[{"codec_type":"audio","codec_name":"mp3","channels":1,"sample_rate":"48000"}],"format":{"format_name":"mp3"}});
        assert!(probe(&value).unwrap().duration_ms.is_none());
        value["format"]["duration"] = json!("NaN");
        assert!(probe(&value).unwrap().duration_ms.is_none());
        value["streams"][0]["channels"] = json!(0);
        assert!(probe(&value).is_err());
        assert!(subtitle_cues("1\n00:00:00,000 --> 00:00:01,000\nOne\n\n2\n00:00:00,500 --> 00:00:01,500\nOverlap",2000,0).is_err());
    }
}
