pub mod audio;
pub mod batch;
mod browser;
pub mod commands;
pub mod config;
pub mod delivery;
pub mod documents;
pub mod egress;
pub mod jobs;
pub mod media;
pub mod network;
pub mod operator;
pub mod process;
pub mod projection;
pub mod recovery;
pub mod runtime;
pub mod sources;
pub mod speech;
pub mod storage;
pub mod telegram;
pub mod transforms;
pub mod translation;
mod translation_protocol;
pub mod workflows;
pub mod x;

pub fn digest(bytes: impl AsRef<[u8]>) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}
