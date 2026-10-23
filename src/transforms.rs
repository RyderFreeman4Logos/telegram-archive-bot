//! Lease-coordinated, versioned transformations; a CAS digest alone is not a work identity.
use crate::{
    digest,
    jobs::{Failure, Lease, fence},
    storage::{Store, now},
};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, params};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{future::Future, path::Path, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub(crate) enum Reuse {
    Validated,
    Upstream,
}
pub(crate) struct Transform {
    pub identity: Value,
    pub reuse: Reuse,
    pub seconds: u64,
}
enum Claim<T> {
    Hit(T),
    Wait,
    Run(String),
}
pub(crate) fn executable(path: &Path) -> Result<Value> {
    use sha2::{Digest, Sha256};
    use std::{fs::File, os::unix::fs::PermissionsExt};
    ensure!(path.is_absolute(), Failure::InvalidInput);
    let resolved = path.canonicalize()?;
    let mut file = File::open(&resolved)?;
    ensure!(
        file.metadata()?.is_file() && file.metadata()?.permissions().mode() & 0o111 != 0,
        Failure::InvalidInput
    );
    let mut hash = Sha256::new();
    std::io::copy(&mut file, &mut hash)?;
    Ok(
        serde_json::json!({"configured_path":path,"resolved_path":resolved,"sha256":format!("{:x}",hash.finalize())}),
    )
}
impl Store {
    fn transform_claim<T: Serialize + DeserializeOwned>(
        &self,
        lease: &Lease,
        identity: &Value,
        reuse: Reuse,
        validate: &impl Fn(&T) -> Result<bool>,
    ) -> Result<Claim<T>> {
        self.heartbeat(lease)?;
        let fingerprint = digest(serde_json::to_vec(identity)?);
        let cached: Option<(String, String)> = if matches!(reuse, Reuse::Validated) {
            if lease.job.force {
                self.connection()?.query_row("SELECT id,output FROM transformation_runs WHERE fingerprint=?1 AND owner_job=?2 AND state='complete' AND eligible=1 ORDER BY rowid DESC LIMIT 1",params![fingerprint,lease.job.id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?
            } else {
                self.connection()?.query_row("SELECT r.id,r.output FROM transformation_selections s JOIN transformation_runs r ON r.id=s.run_id WHERE s.fingerprint=?1 AND r.state='complete' AND r.eligible=1",[&fingerprint],|r|Ok((r.get(0)?,r.get(1)?))).optional()?
            }
        } else {
            None
        };
        let considered_run = cached.as_ref().map(|(run, _)| run.clone());
        if let Some((run, output)) = &cached {
            let output: T = serde_json::from_str(output)?;
            if validate(&output)? {
                let mut conn = self.connection()?;
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                fence(&tx, lease, now())?;
                tx.execute(
                    "INSERT INTO transformation_uses VALUES(?1,?2,?3,?4) ON CONFLICT DO NOTHING",
                    params![lease.job.id, fingerprint, run, now()],
                )?;
                tx.commit()?;
                return Ok(Claim::Hit(output));
            }
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let time = now();
        fence(&tx, lease, time)?;
        if let Some((run,active))=tx.query_row("SELECT r.id,r.state='running' AND r.lease_until>?2 AND j.state='running' AND j.generation=r.owner_generation AND j.lease_until>?2 AND j.deadline>?2 AND j.cancel_requested=0 FROM transformation_slots s JOIN transformation_runs r ON r.id=s.run_id JOIN jobs j ON j.id=r.owner_job WHERE s.fingerprint=?1",params![fingerprint,time],|r|Ok((r.get::<_,String>(0)?,r.get::<_,bool>(1)?))).optional()?{
            if active {tx.commit()?;return Ok(Claim::Wait);}
            tx.execute("UPDATE transformation_runs SET state='failed',finished_at=?1,error='owner_lost' WHERE id=?2 AND state='running'",params![time,run])?;tx.execute("DELETE FROM transformation_slots WHERE fingerprint=?1",[&fingerprint])?;
        }
        // Recheck the selection after acquiring the writer; another owner may just have finished.
        if matches!(reuse, Reuse::Validated) {
            let changed: Option<String> = if lease.job.force {
                tx.query_row("SELECT id FROM transformation_runs WHERE fingerprint=?1 AND owner_job=?2 AND state='complete' AND eligible=1 ORDER BY rowid DESC LIMIT 1",params![fingerprint,lease.job.id],|r|r.get(0)).optional()?
            } else {
                tx.query_row(
                    "SELECT run_id FROM transformation_selections WHERE fingerprint=?1",
                    [&fingerprint],
                    |r| r.get(0),
                )
                .optional()?
            };
            if changed.is_some() && changed != considered_run {
                tx.commit()?;
                return Ok(Claim::Wait);
            }
        }
        let id: String = tx.query_row("SELECT lower(hex(randomblob(32)))", [], |r| r.get(0))?;
        tx.execute("INSERT INTO transformation_runs(id,fingerprint,identity,owner_job,owner_generation,state,lease_until,created_at) VALUES(?1,?2,?3,?4,(SELECT generation FROM jobs WHERE id=?4),'running',?5,?6)",params![id,fingerprint,serde_json::to_string(identity)?,lease.job.id,time+lease.config.limits.lease_seconds as i64,time])?;
        tx.execute(
            "INSERT INTO transformation_slots VALUES(?1,?2)",
            params![fingerprint, id],
        )?;
        tx.commit()?;
        Ok(Claim::Run(id))
    }
    fn transform_heartbeat(&self, lease: &Lease, run: &str) -> Result<()> {
        self.heartbeat(lease)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let time = now();
        fence(&tx, lease, time)?;
        ensure!(tx.execute("UPDATE transformation_runs SET lease_until=?1 WHERE id=?2 AND state='running' AND lease_until>?3 AND owner_job=?4 AND owner_generation=(SELECT generation FROM jobs WHERE id=?4) AND EXISTS(SELECT 1 FROM transformation_slots WHERE run_id=?2)",params![time+lease.config.limits.lease_seconds as i64,run,time,lease.job.id])?==1,Failure::LeaseLost);
        tx.commit()?;
        Ok(())
    }
    fn transform_failed(&self, run: &str) -> Result<()> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute("UPDATE transformation_runs SET state='failed',finished_at=?1,error='attempt_failed' WHERE id=?2 AND state='running'",params![now(),run])?;
        tx.execute("DELETE FROM transformation_slots WHERE run_id=?1", [run])?;
        tx.commit()?;
        Ok(())
    }
    pub(crate) async fn coalesce<T, F, Fut, V>(
        &self,
        lease: &Lease,
        transform: Transform,
        cancel: &CancellationToken,
        validate: V,
        produce: F,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce(String, CancellationToken) -> Fut,
        Fut: Future<Output = Result<T>>,
        V: Fn(&T) -> Result<bool>,
    {
        let Transform {
            identity,
            reuse,
            seconds,
        } = transform;
        ensure!(seconds > 0, Failure::InvalidInput);
        let started = tokio::time::Instant::now();
        let run = loop {
            ensure!(!cancel.is_cancelled(), Failure::Cancelled);
            ensure!(
                started.elapsed() < Duration::from_secs(seconds),
                Failure::Timeout
            );
            match self.transform_claim(lease, &identity, reuse, &validate)? {
                Claim::Hit(output) => return Ok(output),
                Claim::Run(run) => break run,
                Claim::Wait => {
                    tokio::select! {_=cancel.cancelled()=>return Err(Failure::Cancelled.into()),_=tokio::time::sleep(Duration::from_millis(40))=>{}}
                }
            }
        };
        let child = cancel.child_token();
        let pending = produce(run.clone(), child.clone());
        tokio::pin!(pending);
        let timeout =
            tokio::time::sleep(Duration::from_secs(seconds).saturating_sub(started.elapsed()));
        tokio::pin!(timeout);
        let mut tick = tokio::time::interval(Duration::from_secs(
            (lease.config.limits.lease_seconds / 3).max(1),
        ));
        let (outcome, finished) = loop {
            tokio::select! {biased;
                _=cancel.cancelled()=>break (Err(Failure::Cancelled.into()),false),
                _=&mut timeout=>break (Err(Failure::Timeout.into()),false),
                output=&mut pending=>break (output,true),
                _=tick.tick()=>{if let Err(error)=self.transform_heartbeat(lease,&run){break (Err(error),false);}}
            }
        };
        if !finished {
            child.cancel();
            let _ = tokio::time::timeout(
                Duration::from_secs(lease.config.limits.shutdown_seconds),
                &mut pending,
            )
            .await;
        }
        let outcome=outcome.and_then(|output| {
                let eligible = validate(&output)?;
                let mut conn = self.connection()?;
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let time = now();
                fence(&tx, lease, time)?;
                ensure!(tx.execute("UPDATE transformation_runs SET state='complete',output=?1,eligible=?2,finished_at=?3 WHERE id=?4 AND state='running' AND lease_until>?3 AND EXISTS(SELECT 1 FROM transformation_slots WHERE run_id=?4)",params![serde_json::to_string(&output)?,eligible,time,run])?==1,Failure::LeaseLost);
                if eligible {
                    tx.execute("INSERT INTO transformation_selections VALUES(?1,?2) ON CONFLICT(fingerprint) DO UPDATE SET run_id=excluded.run_id",params![digest(serde_json::to_vec(&identity)?),run])?;
                }
                tx.execute("DELETE FROM transformation_slots WHERE run_id=?1", [&run])?;
                tx.commit()?;
                Ok(output)
        });
        if outcome.is_err() {
            let _ = self.transform_failed(&run);
        }
        outcome
    }
}

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct AudioSet {
    pub schema_version: u32,
    pub candidates: Vec<String>,
    pub artifacts: Vec<String>,
}
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct NarrationRequest {
    pub revision: String,
    pub input_digest: String,
    pub input_kind: String,
    pub view_id: Option<String>,
    pub profile: crate::audio::Profile,
    pub repair: crate::audio::Repair,
}
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct DownloadRequest {
    pub revision: String,
    pub url: String,
    pub playlist_limit: Option<usize>,
    pub profile: crate::audio::Profile,
    pub repair: crate::audio::Repair,
}
impl Store {
    fn audio_set_valid(
        &self,
        lease: &Lease,
        output: &AudioSet,
        profile: crate::audio::Profile,
        tools: &Value,
    ) -> Result<bool> {
        ensure!(
            output.schema_version == 1
                && !output.artifacts.is_empty()
                && !output.candidates.is_empty(),
            Failure::InvalidOutput
        );
        for candidate in &output.candidates {
            self.verify_blob(candidate)?;
        }
        for (order, id) in output.artifacts.iter().enumerate() {
            let artifact = self.artifact(id)?;
            ensure!(
                artifact.order == order
                    && artifact.effective_profile == profile
                    && artifact.byte_length <= lease.config.limits.media_bytes
                    && artifact.duration_ms <= lease.config.limits.max_audio_seconds * 1000,
                Failure::InvalidOutput
            );
            self.verify_blob(&artifact.raw_digest)?;
            if let Some(subtitle) = &artifact.subtitle_digest {
                self.verify_blob(subtitle)?;
            }
            for name in ["ffmpeg", "ffprobe"] {
                ensure!(
                    artifact.validation.tools[name].sha256 == tools[name]["sha256"],
                    Failure::InvalidOutput
                );
            }
        }
        Ok(true)
    }
    fn audio_tools(lease: &Lease) -> Result<Value> {
        Ok(
            serde_json::json!({"ffmpeg":executable(&lease.config.tools.ffmpeg)?,"ffprobe":executable(&lease.config.tools.ffprobe)?}),
        )
    }
    pub async fn narrate(
        &self,
        lease: &Lease,
        request: &NarrationRequest,
        cancel: &CancellationToken,
    ) -> Result<AudioSet> {
        use crate::{
            audio::{AudioFinalizer, AudioInput},
            speech::{Md2Audio, SpeechCandidate, SpokenText},
        };
        self.heartbeat(lease)?;
        lease.config.validate()?;
        ensure!(
            lease.config.synthesis.enabled
                && matches!(
                    request.input_kind.as_str(),
                    "original" | "translation" | "summary"
                ),
            Failure::InvalidInput
        );
        let revision = self.revision(&request.revision)?;
        let input = self.read_blob(&request.input_digest, lease.config.limits.input_bytes)?;
        if request.input_kind == "original" {
            ensure!(
                input == revision.content.body.as_bytes(),
                Failure::InvalidInput
            );
        }
        let context = if let Some(id) = &request.view_id {
            let view = self.view(id)?;
            ensure!(
                view.members
                    .iter()
                    .any(|m| m.revision_id.as_ref() == Some(&request.revision)),
                Failure::InvalidInput
            );
            Some(view)
        } else {
            None
        };
        let tools = Self::audio_tools(lease)?;
        let producer = executable(&lease.config.tools.md2audio)?;
        // A Python launcher hash cannot attest its package, local weights or remote TTS deployment.
        let unverified_job = lease
            .config
            .synthesis
            .deployment_revision
            .trim()
            .is_empty()
            .then_some(&lease.job.id);
        let identity = serde_json::json!({"transform":"validated-narration-v1","text_policy":"verbatim-markdown-v1-labels-are-metadata","validation":"audio-finalizer-v1","request":request,"view":context,"synthesis":lease.config.synthesis,"unverified_job":unverified_job,"config":lease.job.config_id,"sampling":"md2audio-explicit-settings","producer":producer,"tools":tools,"snapshot_tools":lease.tools});
        self.coalesce(lease,Transform{identity,reuse:Reuse::Validated,seconds:lease.config.limits.synthesis_seconds+lease.config.limits.media_seconds},cancel,|set:&AudioSet|{
            self.audio_set_valid(lease,set,request.profile,&tools)?;
            ensure!(set.candidates.len()==1,Failure::InvalidOutput);
            for digest in &set.candidates{let candidate:SpeechCandidate=serde_json::from_slice(&self.read_blob(digest,lease.config.limits.input_bytes)?)?;
                ensure!(candidate.revision==request.revision&&candidate.segments.len()==set.artifacts.len(),Failure::InvalidOutput);
                let spoken:SpokenText=serde_json::from_slice(&self.read_blob(&candidate.spoken_digest,lease.config.limits.input_bytes)?)?;
                ensure!(candidate.config_id==lease.job.config_id&&spoken.revision==request.revision&&spoken.input_digest==request.input_digest&&spoken.input_kind==request.input_kind&&serde_json::to_value(&spoken.settings)?==serde_json::to_value(&lease.config.synthesis)?,Failure::InvalidOutput);
                for (segment,id) in candidate.segments.iter().zip(&set.artifacts){let artifact=self.artifact(id)?;ensure!(artifact.raw_digest==segment.blob_digest&&artifact.parent_id==*digest,Failure::InvalidOutput);self.verify_blob(&segment.text_digest)?;}
                ensure!(candidate.segments.iter().all(|s|s.producer_digest==producer["sha256"]),Failure::InvalidOutput);
            }Ok(true)
        },|run,token|async move{
            let candidate=Md2Audio{store:self.clone()}.synthesize(lease,&request.revision,&request.input_digest,&request.input_kind,&token).await?;
            let speech:SpeechCandidate=serde_json::from_slice(&self.read_blob(&candidate,lease.config.limits.input_bytes)?)?;
            let inputs:Vec<_>=speech.segments.iter().map(|s|AudioInput{blob_digest:s.blob_digest.clone(),parent_id:candidate.clone(),producer_tools:serde_json::json!({"md2audio":s.producer_digest,"spoken":speech.spoken_digest}),order:s.order,subtitle_digest:s.subtitle_digest.clone()}).collect();
            let artifacts=AudioFinalizer{store:self.clone()}.finalize(lease,&format!("narration:{run}"),&inputs,request.profile,request.repair,&token).await?;
            Ok(AudioSet{schema_version:1,candidates:vec![candidate],artifacts:artifacts.into_iter().map(|a|a.artifact_id).collect()})
        }).await
    }
    pub async fn download_audio(
        &self,
        lease: &Lease,
        request: &DownloadRequest,
        relay: &Path,
        cancel: &CancellationToken,
    ) -> Result<AudioSet> {
        use crate::{
            audio::{AudioFinalizer, AudioInput},
            media::{MediaCandidate, MediaDownloader},
        };
        self.heartbeat(lease)?;
        ensure!(lease.config.sources.media_enabled, Failure::InvalidInput);
        self.revision(&request.revision)?;
        let tools = Self::audio_tools(lease)?;
        let producer = executable(&lease.config.tools.yt_dlp)?;
        let identity = serde_json::json!({"transform":"validated-download-v1","validation":"audio-finalizer-v1","request":request,"config":lease.job.config_id,"yt_dlp":producer,"bubblewrap":executable(&lease.config.tools.bubblewrap)?,"relay":executable(relay)?,"tools":tools,"snapshot_tools":lease.tools});
        self.coalesce(lease,Transform{identity,reuse:Reuse::Validated,seconds:lease.config.limits.download_seconds+lease.config.limits.media_seconds},cancel,|set:&AudioSet|{
            self.audio_set_valid(lease,set,request.profile,&tools)?;
            ensure!(set.candidates.len()==set.artifacts.len(),Failure::InvalidOutput);
            for (digest,id) in set.candidates.iter().zip(&set.artifacts){let candidate:MediaCandidate=serde_json::from_slice(&self.read_blob(digest,lease.config.limits.input_bytes)?)?;let artifact=self.artifact(id)?;
                ensure!(candidate.source_revision==request.revision&&candidate.config_id==lease.job.config_id&&candidate.producer_digest==producer["sha256"]&&candidate.blob_digest==artifact.raw_digest&&artifact.parent_id==*digest,Failure::InvalidOutput);
            }Ok(true)
        },|run,token|async move{
            let candidates=MediaDownloader{store:self.clone(),relay_executable:relay.to_owned()}.download(lease,&request.revision,&request.url,request.playlist_limit,&token).await?;
            let mut inputs=vec![];for digest in &candidates{let candidate:MediaCandidate=serde_json::from_slice(&self.read_blob(digest,lease.config.limits.input_bytes)?)?;
                inputs.push(AudioInput{blob_digest:candidate.blob_digest,parent_id:digest.clone(),producer_tools:serde_json::json!({"yt_dlp":candidate.producer_digest,"tools":candidate.tools}),order:inputs.len(),subtitle_digest:None});
            }
            let artifacts=AudioFinalizer{store:self.clone()}.finalize(lease,&format!("download:{run}"),&inputs,request.profile,request.repair,&token).await?;
            Ok(AudioSet{schema_version:1,candidates,artifacts:artifacts.into_iter().map(|a|a.artifact_id).collect()})
        }).await
    }
}

#[cfg(test)]
#[path = "transforms_tests.rs"]
mod tests;
