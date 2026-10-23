pub use crate::translation_protocol::{BridgeReport, HYMT_REVISION};
use crate::{
    digest,
    jobs::{Failure, Lease, Phase, supervise_lease},
    process::{Tool, ToolFailure, confined_file},
    sources::Revision,
    storage::{Store, now, private_dir},
};
use adk_graph::{END, ExecutionConfig, NodeOutput, START, StateGraph, error::GraphError};
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranslationManifest {
    pub schema_version: u32,
    pub workflow: String,
    pub source_revision: String,
    pub config_id: String,
    pub language: String,
    pub profile: String,
    pub output_digest: String,
    pub normalized_source_digest: String,
    pub bridge_digest: String,
    pub report: BridgeReport,
    pub partial: bool,
    pub semantic_fidelity_verified: bool,
}

#[async_trait::async_trait]
pub trait Translator: Send + Sync {
    async fn translate(
        &self,
        lease: &Lease,
        revision: &Revision,
        language: &str,
        profile: &str,
        cancel: &CancellationToken,
    ) -> Result<String>;
}

pub struct HymtTranslator {
    pub store: Store,
}

fn private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

fn job_config(lease: &Lease, key: &str) -> Result<String> {
    let t = &lease.config.translation;
    Ok(toml::to_string(&json!({
        "endpoint":{"url":t.endpoint,"api_key":key,"model":t.model,"profile":t.profile,"backend":t.backend},
        "backend":{"total_context":t.context_tokens,"per_request_context":t.context_tokens,"parallel_slots":1},
        "translation":{"max_output_tokens":(t.context_tokens / 4).min(4096),"concurrency":1,"stream":true,"config_version":1,
            "timeout":lease.config.limits.llm_seconds as f64,"max_retries":0,"max_source_tokens_per_segment":384,
            "strict_token_budget":false,"strict_backend_preflight":false,"language_detection":true,"force_translate_all":false},
        "completeness":{"max_retries":0,"warn_only":true},"cache":{"enabled":true}
    }))?)
}

fn read_output(root: &Path, name: &str, limit: u64) -> Result<String> {
    let path = confined_file(root, Path::new(name)).map_err(|_| Failure::InvalidOutput)?;
    ensure!(path.metadata()?.len() <= limit, Failure::InvalidOutput);
    String::from_utf8(fs::read(path)?).map_err(|_| Failure::InvalidOutput.into())
}

fn validate(
    report: &BridgeReport,
    source: &str,
    normalized: &str,
    output: &str,
    max_segments: usize,
) -> Result<()> {
    ensure!(
        report.schema_version == 1
            && report.hymt_revision == HYMT_REVISION
            && report.source_digest == digest(source)
            && report.normalized_source_digest == digest(normalized)
            && report.output_digest == digest(output)
            && !output.trim().is_empty()
            && report.inference_fingerprint.len() == 64
            && report
                .inference_fingerprint
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            && report.segments <= max_segments
            && report.segment_input_tokens.len() == report.segments
            && matches!(report.counting_source.as_str(), "local" | "approximate")
            && report
                .completed_segments
                .iter()
                .all(|n| *n < report.segments)
            && report
                .degraded_segments
                .iter()
                .all(|n| *n > 0 && *n <= report.segments)
            && report.preserved.len() <= 50000
            && report.retained.len() <= 50000,
        Failure::InvalidOutput
    );
    for region in &report.preserved {
        let text = normalized
            .get(region.source_start..region.source_end)
            .ok_or(Failure::InvalidOutput)?;
        ensure!(
            !text.is_empty()
                && digest(text) == region.digest
                && output.contains(text.trim())
                && matches!(region.kind.as_str(), "code" | "target_language"),
            Failure::InvalidOutput
        );
    }
    for region in &report.retained {
        let source = normalized
            .get(region.source_start..region.source_end)
            .ok_or(Failure::InvalidOutput)?;
        let output = output
            .get(region.output_start..region.output_end)
            .ok_or(Failure::InvalidOutput)?;
        ensure!(
            !source.is_empty() && source == output && !report.degraded_segments.is_empty(),
            Failure::InvalidOutput
        );
    }
    Ok(())
}

fn tool_failure(error: &crate::process::ToolError) -> Failure {
    match error.kind {
        ToolFailure::Missing => Failure::ToolMissing,
        ToolFailure::Timeout => Failure::Timeout,
        ToolFailure::Cancelled => Failure::Cancelled,
        ToolFailure::Nonzero => error
            .report
            .as_ref()
            .and_then(|r| serde_json::from_str::<Value>(&r.stdout).ok())
            .and_then(|v| v.get("error").cloned())
            .and_then(|v| serde_json::from_value::<Failure>(v).ok())
            .filter(|f| {
                matches!(
                    f,
                    Failure::Network
                        | Failure::RateLimited
                        | Failure::Unavailable
                        | Failure::Timeout
                        | Failure::ModelTruncated
                        | Failure::InvalidOutput
                )
            })
            .unwrap_or(Failure::ToolExit),
        _ => Failure::InvalidOutput,
    }
}

#[async_trait::async_trait]
impl Translator for HymtTranslator {
    async fn translate(
        &self,
        lease: &Lease,
        revision: &Revision,
        language: &str,
        profile: &str,
        cancel: &CancellationToken,
    ) -> Result<String> {
        self.store.heartbeat(lease)?;
        let config = &lease.config;
        let t = &config.translation;
        let budget = config.limits.llm_seconds;
        ensure!(
            t.enabled
                && language == t.language
                && profile == t.profile
                && revision.content.body.len() as u64 <= config.limits.input_bytes,
            Failure::InvalidInput
        );
        ensure!(
            lease
                .job
                .deadline
                .is_some_and(|d| d - now() >= budget as i64),
            Failure::Timeout
        );
        // Reload from canonical storage rather than trusting an injected body for a real revision ID.
        let original = self.store.revision(&revision.id)?;
        ensure!(original.content == revision.content, Failure::InvalidInput);
        let workspace = self.store.workspace(&lease.job.id)?;
        private_file(
            &workspace.path().join("source.md"),
            revision.content.body.as_bytes(),
        )?;
        let key = t
            .key
            .as_ref()
            .map(|r| r.read())
            .transpose()
            .map_err(|_| Failure::InvalidInput)?
            .unwrap_or_default();
        private_file(
            &workspace.path().join("job.toml"),
            job_config(lease, &key)?.as_bytes(),
        )?;
        let cache = self.store.root().join("cache");
        private_dir(&cache)?;
        let cache = cache.join("hymt");
        private_dir(&cache)?;
        let cache = cache.join(format!("{}.sqlite", lease.job.config_id));
        // Never follow an operator-created cache symlink into another database.
        let cache_file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&cache)?;
        ensure!(
            cache_file.metadata()?.is_file()
                && cache_file.metadata()?.permissions().mode() & 0o077 == 0,
            Failure::InvalidInput
        );
        let request_key = digest(format!(
            "translation-v1:{HYMT_REVISION}:{}:{}:{}:{}",
            revision.id, lease.job.config_id, lease.job.id, lease.job.attempt
        ));
        let attempt = self.store.model_attempt(lease, &request_key, &json!({"revision":revision.id,"language":language,"profile":profile,"hymt_revision":HYMT_REVISION}))?;
        let tool = Tool {
            executable: config.tools.hymt_bridge.clone(),
            args: vec![
                "job.toml".into(),
                "source.md".into(),
                language.into(),
                cache.into_os_string(),
                config.limits.input_bytes.to_string().into(),
                config.limits.graph_nodes.to_string().into(),
            ],
            environment: BTreeMap::new(),
            directory: workspace.path().into(),
            timeout: Duration::from_secs(budget),
            log_bytes: config.limits.log_bytes,
            work_bytes: config.limits.work_bytes,
            secrets: vec![key],
        };
        let run = tool
            .run(cancel)
            .await
            .map_err(|e| anyhow::Error::from(tool_failure(&e)));
        let outcome = run.and_then(|run| {
            let report: BridgeReport = serde_json::from_str(&run.stdout).map_err(|_| Failure::InvalidOutput)?;
            let normalized = read_output(workspace.path(), "normalized-source.md", config.limits.input_bytes)?;
            let output = read_output(workspace.path(), "translation.md", config.limits.input_bytes)?;
            validate(&report, &revision.content.body, &normalized, &output, config.limits.graph_nodes)?;
            self.store.heartbeat(lease)?;
            let output = self.store.publish_bytes(output.as_bytes())?;
            let normalized = self.store.publish_bytes(normalized.as_bytes())?;
            let manifest = TranslationManifest { schema_version:1, workflow:"translation-v1".into(),source_revision:revision.id.clone(),config_id:lease.job.config_id.clone(),
                language:language.into(),profile:profile.into(),output_digest:output.digest,normalized_source_digest:normalized.digest,bridge_digest:run.executable_digest,
                partial:!report.degraded_segments.is_empty() || !report.retained.is_empty(),semantic_fidelity_verified:false,report };
            // Upstream alone admits verified cache reads. Unverified generations get an attempt-local identity.
            let key = digest(serde_json::to_vec(&json!({"version":"translation-v1","hymt":HYMT_REVISION,"revision":revision.id,"config":lease.job.config_id,
                "bridge":manifest.bridge_digest,"inference":manifest.report.inference_fingerprint,"output":manifest.output_digest,
                "unverified_attempt":if manifest.report.cache_verified { None } else {Some(&request_key)} }))?);
            let digest = self.store.publish_derivative(lease, &key, "translation", &revision.id, &manifest)?;
            Ok::<_, anyhow::Error>((digest, manifest.output_digest))
        });
        let (status, response) = match &outcome {
            Ok((_, output)) => ("validated".to_string(), Some(output.clone())),
            Err(error) => (
                serde_json::to_string(
                    error
                        .downcast_ref::<Failure>()
                        .unwrap_or(&Failure::Internal),
                )?,
                None,
            ),
        };
        self.store.connection()?.execute(
            "UPDATE model_attempts SET response_digest=?1,status=?2 WHERE id=?3",
            rusqlite::params![response, status, attempt],
        )?;
        outcome.map(|(digest, _)| digest)
    }
}

pub struct TranslationWorkflow {
    pub store: Store,
    pub translator: Arc<dyn Translator>,
}
impl TranslationWorkflow {
    pub async fn resume_next(store: Store, shutdown: CancellationToken) -> Result<bool> {
        let Some(lease) = store.claim_phase(Phase::Translate)? else {
            return Ok(false);
        };
        let workflow = Self {
            translator: Arc::new(HymtTranslator {
                store: store.clone(),
            }),
            store: store.clone(),
        };
        let executing = &lease;
        let result = supervise_lease(
            store.clone(),
            lease.clone(),
            shutdown,
            |cancel| async move {
                workflow.run(executing, cancel).await.map_err(|e| {
                    e.downcast_ref::<Failure>()
                        .copied()
                        .unwrap_or(Failure::Internal)
                })
            },
        )
        .await;
        if let Err(failure) = result {
            if failure != Failure::LeaseLost {
                store.fail_phase(&lease, failure)?;
            }
            return Err(failure.into());
        }
        Ok(true)
    }

    pub async fn run(&self, lease: &Lease, cancel: CancellationToken) -> Result<()> {
        self.store.heartbeat(lease)?;
        let outputs = self.store.phase_outputs(&lease.job.id)?;
        let view_id = outputs
            .get("fetch")
            .and_then(|v| v.get("view_id"))
            .and_then(Value::as_str)
            .ok_or(Failure::InvalidInput)?;
        let view = self.store.view(view_id)?;
        if !lease.config.translation.enabled {
            self.store.finish_phase(
                lease,
                &json!({"enabled":false,"view_id":view_id,"translations":[]}),
                Some(Phase::Download),
                false,
            )?;
            return Ok(());
        }
        let revisions: Vec<_> = view
            .members
            .iter()
            .filter_map(|m| m.revision_id.clone())
            .collect();
        ensure!(!revisions.is_empty(), Failure::InvalidInput);
        let budget = lease.config.limits.llm_seconds;
        ensure!(
            lease
                .job
                .deadline
                .is_some_and(|d| d - now() >= budget as i64),
            Failure::Timeout
        );
        let deadline = Instant::now() + Duration::from_secs(budget);
        let names: Vec<_> = (0..revisions.len())
            .map(|i| format!("translation_{i}"))
            .collect();
        let mut graph =
            StateGraph::with_channels(&names.iter().map(String::as_str).collect::<Vec<_>>());
        for (index, revision) in revisions.iter().enumerate() {
            let translator = self.translator.clone();
            let store = self.store.clone();
            let lease = lease.clone();
            let cancel = cancel.clone();
            let revision = revision.clone();
            let name = names[index].clone();
            graph = graph
                .add_node_fn(&names[index], move |_| {
                    let translator = translator.clone();
                    let store = store.clone();
                    let lease = lease.clone();
                    let cancel = cancel.clone();
                    let revision = revision.clone();
                    let name = name.clone();
                    async move {
                        let run = async {
                            let revision = store.revision(&revision)?;
                            translator
                                .translate(
                                    &lease,
                                    &revision,
                                    &lease.config.translation.language,
                                    &lease.config.translation.profile,
                                    &cancel,
                                )
                                .await
                        };
                        match run.await {
                            Ok(output) => Ok(NodeOutput::new().with_update(&name, output)),
                            Err(error) => Err(GraphError::Other(
                                serde_json::to_string(
                                    error
                                        .downcast_ref::<Failure>()
                                        .unwrap_or(&Failure::Internal),
                                )
                                .unwrap(),
                            )),
                        }
                    }
                })
                .add_edge(
                    if index == 0 { START } else { &names[index - 1] },
                    &names[index],
                );
        }
        let graph = graph
            .add_edge(names.last().unwrap(), END)
            .compile()?
            .with_strict_channels()
            .with_recursion_limit(names.len() + 2);
        let run = graph.invoke(
            HashMap::new(),
            ExecutionConfig::new(&format!("translation-{}", lease.job.id))
                .with_recursion_limit(names.len() + 2),
        );
        let state = tokio::select! {biased;
            _=cancel.cancelled()=>return Err(Failure::Cancelled.into()),
            result=tokio::time::timeout(deadline.saturating_duration_since(Instant::now()),run)=>match result {
                Err(_)=>return Err(Failure::Timeout.into()),
                Ok(Err(GraphError::NodeExecutionFailed{message,..}))=>return Err(serde_json::from_str::<Failure>(&message).unwrap_or(Failure::Internal).into()),
                Ok(result)=>result?,
            }
        };
        let mut partial = false;
        let mut manifests = vec![];
        for name in &names {
            let hash = state
                .get(name)
                .and_then(Value::as_str)
                .ok_or(Failure::InvalidOutput)?;
            let manifest: TranslationManifest = serde_json::from_slice(
                &self
                    .store
                    .read_blob(hash, lease.config.limits.log_bytes as u64 * 4)?,
            )?;
            partial |= manifest.partial;
            manifests.push(hash);
        }
        self.store.finish_phase(
            lease,
            &json!({"enabled":true,"view_id":view_id,"translations":manifests}),
            Some(Phase::Download),
            partial,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bridge_reports_must_match_bytes_and_valid_unicode_ranges() -> Result<()> {
        let source = "λ😀";
        let mut report: BridgeReport = serde_json::from_value(json!({
            "schema_version":1,"hymt_revision":HYMT_REVISION,"source_digest":digest(source),"normalized_source_digest":digest(source),
            "output_digest":digest(source),"inference_fingerprint":digest("fixture"),"cache_verified":false,"segments":1,
            "segment_input_tokens":[100],"counting_source":"approximate","completed_segments":[0],"degraded_segments":[1],
            "preserved":[],"retained":[{"paragraph":0,"source_start":0,"source_end":source.len(),"output_start":0,"output_end":source.len()}]
        }))?;
        validate(&report, source, source, source, 10)?;
        report.retained[0].source_start = 1;
        assert!(validate(&report, source, source, source, 10).is_err());
        report.retained.clear();
        report.output_digest = digest("different");
        assert!(validate(&report, source, source, source, 10).is_err());
        Ok(())
    }
}
