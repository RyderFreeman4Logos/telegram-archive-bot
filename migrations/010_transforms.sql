CREATE TABLE transformation_runs (
 id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
 identity TEXT NOT NULL CHECK(json_valid(identity)),
 owner_job TEXT NOT NULL REFERENCES jobs(id), owner_generation INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('running','complete','failed')),
 lease_until INTEGER NOT NULL, output TEXT CHECK(output IS NULL OR json_valid(output)),
 eligible INTEGER NOT NULL DEFAULT 0 CHECK(eligible IN (0,1)),
 created_at INTEGER NOT NULL, finished_at INTEGER, error TEXT
) STRICT;
CREATE INDEX transformation_history ON transformation_runs(fingerprint,owner_job,state);
CREATE TABLE transformation_slots (
 fingerprint TEXT PRIMARY KEY, run_id TEXT NOT NULL UNIQUE REFERENCES transformation_runs(id)
) STRICT;
CREATE TABLE transformation_selections (
 fingerprint TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES transformation_runs(id)
) STRICT;
CREATE TABLE transformation_uses (
 job_id TEXT NOT NULL REFERENCES jobs(id), fingerprint TEXT NOT NULL,
 run_id TEXT NOT NULL REFERENCES transformation_runs(id), used_at INTEGER NOT NULL,
 PRIMARY KEY(job_id,fingerprint,run_id)
) STRICT;
CREATE TRIGGER closed_transformation_run BEFORE UPDATE ON transformation_runs
WHEN OLD.state!='running' BEGIN SELECT RAISE(ABORT,'closed transformation run'); END;
