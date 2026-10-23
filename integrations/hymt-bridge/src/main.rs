use anyhow::{Result, ensure};
use hymt_cache::HistoryDB;
use hymt_client::{ClientError, TranslationClient};
use hymt_core::{
    config::HotConfig,
    language::SectionKind,
    templates::{PromptOpts, TemplateType},
};
use hymt_segment::Segmenter;
use hymt_translate::{StreamEvent, TranslationCtx, plan_translation, translate_text_stream};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fs, path::Path};
use tokio::sync::mpsc;

#[path = "../../../src/translation_protocol.rs"]
mod protocol;
use protocol::{BridgeReport, HYMT_REVISION, PreservedRegion, RetainedRegion};

fn digest(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

fn failure(error: &anyhow::Error) -> &'static str {
    for cause in error.chain() {
        if let Some(error) = cause.downcast_ref::<ClientError>() {
            return match error {
                ClientError::Truncated => "model_truncated",
                ClientError::Http { status: 429, .. } => "rate_limited",
                ClientError::Http {
                    status: 500..=599, ..
                } => "unavailable",
                ClientError::Request(e) if e.is_timeout() => "timeout",
                ClientError::Request(_) => "network",
                _ => "invalid_output",
            };
        }
        // This pinned hymt wrapper formats ClientError instead of retaining its type.
        let message = cause.to_string();
        if let Some(wrapped) = message
            .strip_prefix("HTTP translation failed for segment ")
            .and_then(|s| s.split_once("): ").map(|(_, cause)| cause))
        {
            if let Some(status) = wrapped
                .strip_prefix("HTTP ")
                .and_then(|s| s.split_once(": ").map(|(status, _)| status))
                .and_then(|s| s.parse::<u16>().ok())
            {
                return match status {
                    429 => "rate_limited",
                    500..=599 => "unavailable",
                    _ => "invalid_output",
                };
            }
            if wrapped.starts_with("request error: ") {
                return "network";
            }
        }
    }
    "invalid_output"
}

async fn run(args: &[String]) -> Result<BridgeReport> {
    ensure!(
        args.len() == 6,
        "expected config, input, language, cache, byte and segment limits"
    );
    let limit: usize = args[4].parse()?;
    let max_segments: usize = args[5].parse()?;
    ensure!(limit > 0 && max_segments > 0, "invalid limits");
    ensure!(fs::metadata(&args[1])?.len() <= limit as u64, "input limit");
    let source = fs::read_to_string(&args[1])?;
    ensure!(!source.trim().is_empty(), "empty input");
    // The host creates this private file; never create or reload interactive configuration.
    ensure!(Path::new(&args[0]).is_file(), "missing job configuration");
    let config = HotConfig::from_path(&args[0])?.detached_snapshot();
    ensure!(
        config.network_max_retries() == 0 && config.completeness_max_retries() == 0,
        "nested retries forbidden"
    );
    let client = TranslationClient::new(config.clone())?;
    client.preflight_backend().await?;
    let segmenter = Segmenter::fallback();
    let history = HistoryDB::new(&args[3]);
    let template = TemplateType::Default;
    let opts = PromptOpts::default();
    let plan = plan_translation(&source, &args[2], &config, &segmenter, &template, &opts)?;
    ensure!(plan.segments.len() <= max_segments, "segment limit");
    let normalized: String = match &plan.document_plan {
        Some(document) => document.sections.iter().map(|s| s.text.as_str()).collect(),
        None => source.clone(),
    };
    ensure!(normalized.len() <= limit, "normalized source limit");
    let mut preserved = vec![];
    let mut offset = 0;
    if let Some(document) = &plan.document_plan {
        for (section, part) in document.sections.iter().enumerate() {
            if !part.should_translate
                && part.kind != SectionKind::Separator
                && !part.text.is_empty()
            {
                preserved.push(PreservedRegion {
                    section,
                    kind: if part.kind == SectionKind::Code {
                        "code"
                    } else {
                        "target_language"
                    }
                    .into(),
                    digest: digest(&part.text),
                    source_start: offset,
                    source_end: offset + part.text.len(),
                });
            }
            offset += part.text.len();
        }
    }
    let ctx = TranslationCtx {
        config: &config,
        client: &client,
        segmenter: &segmenter,
        history: &history,
        cache_enabled: true,
    };
    let (sender, mut events) = mpsc::channel(16);
    let translation = translate_text_stream(&source, &args[2], &template, &opts, &ctx, sender);
    tokio::pin!(translation);
    let mut completed = BTreeSet::new();
    let mut closed = false;
    let outcome = loop {
        tokio::select! {
            result = &mut translation => break result?,
            event = events.recv(), if !closed => match event {
                Some(StreamEvent::SegmentDone(index)) => { ensure!(index < plan.segments.len(), "invalid progress"); completed.insert(index); },
                Some(StreamEvent::Token(_)) | Some(StreamEvent::AllDone(_)) => (),
                None => closed = true,
            }
        }
    };
    // Drain queued progress after success; token or AllDone arrival alone is never success.
    while let Ok(event) = events.try_recv() {
        if let StreamEvent::SegmentDone(index) = event {
            completed.insert(index);
        }
    }
    ensure!(
        !outcome.text.trim().is_empty() && outcome.text.len() <= limit,
        "invalid output size"
    );
    // A protected URL or code block must not disguise an entirely empty translation.
    ensure!(
        plan.segments.is_empty()
            || outcome.text.trim()
                != plan
                    .reconstruct(&vec![String::new(); plan.segments.len()])
                    .trim(),
        "no translated payload"
    );
    for region in &preserved {
        let text = normalized
            .get(region.source_start..region.source_end)
            .ok_or_else(|| anyhow::anyhow!("invalid source range"))?;
        ensure!(
            outcome.text.contains(text.trim()),
            "protected region changed"
        );
    }
    let retained = outcome
        .retained_regions
        .iter()
        .map(|region| RetainedRegion {
            paragraph: region.paragraph_index,
            source_start: region.source_bytes.start,
            source_end: region.source_bytes.end,
            output_start: region.output_bytes.start,
            output_end: region.output_bytes.end,
        })
        .collect();
    let options = hymt_translate::translate::template_options_hash(
        &opts,
        config.document_translation_policy(),
    );
    let fingerprint = config.inference_fingerprint("default", &options)?;
    let report = BridgeReport {
        schema_version: 1,
        hymt_revision: HYMT_REVISION.into(),
        source_digest: digest(&source),
        normalized_source_digest: digest(&normalized),
        output_digest: digest(&outcome.text),
        inference_fingerprint: fingerprint.hash().into(),
        cache_verified: fingerprint.is_cache_verified(),
        segments: plan.segments.len(),
        segment_input_tokens: plan.token_budget.segment_input_tokens,
        counting_source: plan.token_budget.counting_source.as_str().into(),
        completed_segments: completed.into_iter().collect(),
        degraded_segments: outcome.completeness_degraded_segments,
        preserved,
        retained,
    };
    fs::write("normalized-source.md", normalized)?;
    fs::write("translation.md", outcome.text)?;
    Ok(report)
}

#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!(
            "archive-hymt-bridge {} hymt={HYMT_REVISION}",
            env!("CARGO_PKG_VERSION")
        );
        return;
    }
    let args: Vec<_> = std::env::args().skip(1).collect();
    match run(&args).await {
        Ok(report) => println!("{}", serde_json::to_string(&report).unwrap()),
        Err(error) => {
            println!("{}", serde_json::json!({"error":failure(&error)}));
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classify_only_the_pinned_wrapper_without_exposing_its_body() {
        for (status, expected) in [
            (429, "rate_limited"),
            (503, "unavailable"),
            (401, "invalid_output"),
        ] {
            let error = anyhow::anyhow!(
                "HTTP translation failed for segment 2 (source_unicode_scalars=10, approx_source_tokens=4): HTTP {status}: private body mentioning HTTP 429: retry"
            );
            assert_eq!(failure(&error), expected);
            assert_eq!(
                failure(&error.context("segment translation failed")),
                expected
            );
        }
        assert_eq!(
            failure(&anyhow::anyhow!(
                "unrelated private body HTTP 503: unavailable"
            )),
            "invalid_output"
        );
    }
}
