pub mod config;
pub mod jobs;
pub mod sources;
pub mod storage;
pub mod telegram;

pub fn digest(bytes: impl AsRef<[u8]>) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}
