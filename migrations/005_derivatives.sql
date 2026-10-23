CREATE TABLE derivatives (
 derivation_key TEXT PRIMARY KEY,
 kind TEXT NOT NULL, manifest_digest TEXT NOT NULL REFERENCES blobs(digest),
 config_id TEXT NOT NULL REFERENCES config_snapshots(id),
 input_digest TEXT NOT NULL, created_at INTEGER NOT NULL
) STRICT;
CREATE TRIGGER immutable_derivatives BEFORE UPDATE ON derivatives
BEGIN SELECT RAISE(ABORT,'immutable derivative'); END;
CREATE TABLE model_attempts (
 id INTEGER PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
 derivation_key TEXT NOT NULL, request_digest TEXT NOT NULL REFERENCES blobs(digest),
 response_digest TEXT REFERENCES blobs(digest), status TEXT NOT NULL,
 created_at INTEGER NOT NULL
) STRICT;
