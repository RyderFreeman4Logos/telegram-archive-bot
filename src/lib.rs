pub mod config;
pub mod documents;
pub mod jobs;
pub mod network;
pub mod process;
pub mod sources;
pub mod storage;
pub mod telegram;
pub mod x;

pub fn digest(bytes: impl AsRef<[u8]>) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}
