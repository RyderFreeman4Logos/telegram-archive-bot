ALTER TABLE jobs ADD COLUMN partial INTEGER NOT NULL DEFAULT 0 CHECK(partial IN (0,1));
CREATE TABLE phase_attempts (
 job_id TEXT NOT NULL REFERENCES jobs(id), phase TEXT NOT NULL,
 attempts INTEGER NOT NULL CHECK(attempts>0), PRIMARY KEY(job_id,phase)
) STRICT;
CREATE TABLE workflow_runs (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
 config_id TEXT NOT NULL REFERENCES config_snapshots(id), input_digest TEXT NOT NULL,
 graph_thread TEXT NOT NULL UNIQUE, output TEXT CHECK(output IS NULL OR json_valid(output)),
 UNIQUE(job_id,config_id,input_digest)
) STRICT;
