use serde::{Deserialize, Serialize};

pub const HYMT_REVISION: &str = "de670b21b26b3a88a8d711845c9e3d3b9af4c0ba";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreservedRegion {
    pub section: usize,
    pub kind: String,
    pub digest: String,
    pub source_start: usize,
    pub source_end: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedRegion {
    pub paragraph: usize,
    pub source_start: usize,
    pub source_end: usize,
    pub output_start: usize,
    pub output_end: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BridgeReport {
    pub schema_version: u32,
    pub hymt_revision: String,
    pub source_digest: String,
    pub normalized_source_digest: String,
    pub output_digest: String,
    pub inference_fingerprint: String,
    pub cache_verified: bool,
    pub segments: usize,
    pub segment_input_tokens: Vec<usize>,
    pub counting_source: String,
    pub completed_segments: Vec<usize>,
    pub degraded_segments: Vec<usize>,
    pub preserved: Vec<PreservedRegion>,
    pub retained: Vec<RetainedRegion>,
}
