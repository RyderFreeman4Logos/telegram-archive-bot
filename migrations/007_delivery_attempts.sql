ALTER TABLE outbox ADD COLUMN ordinal INTEGER NOT NULL DEFAULT 0;
ALTER TABLE outbox ADD COLUMN lease_until INTEGER;
ALTER TABLE outbox ADD COLUMN retry_base INTEGER NOT NULL DEFAULT 0;
CREATE TABLE delivery_attempts (
 id TEXT PRIMARY KEY, outbox_id TEXT NOT NULL REFERENCES outbox(id),
 ordinal INTEGER NOT NULL, started_at INTEGER NOT NULL, finished_at INTEGER,
 state TEXT NOT NULL, used_file_id INTEGER NOT NULL CHECK(used_file_id IN (0,1)),
 response_digest TEXT REFERENCES blobs(digest), error TEXT,
 UNIQUE(outbox_id,ordinal)
) STRICT;
CREATE TABLE telegram_file_ids (
 bot_id INTEGER NOT NULL, representation TEXT NOT NULL, profile TEXT NOT NULL,
 file_id TEXT NOT NULL, file_unique_id TEXT NOT NULL,
 PRIMARY KEY(bot_id,representation,profile)
) STRICT;
UPDATE outbox SET ordinal=(SELECT count(*) FROM outbox earlier WHERE earlier.job_id=outbox.job_id AND earlier.rowid<outbox.rowid);
CREATE TRIGGER immutable_delivery_payload BEFORE UPDATE OF id,job_id,kind,payload,ordinal ON outbox
BEGIN SELECT RAISE(ABORT,'immutable delivery payload'); END;
CREATE INDEX delivery_job_order ON outbox(job_id,ordinal,state);
CREATE UNIQUE INDEX delivery_logical_order ON outbox(job_id,ordinal);
ALTER TABLE jobs ADD COLUMN force_run INTEGER NOT NULL DEFAULT 0 CHECK(force_run IN (0,1));
CREATE TABLE telegram_cooldowns (
 bot_id INTEGER PRIMARY KEY, until INTEGER NOT NULL
) STRICT;
