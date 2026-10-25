-- DA04 / #71. Canonical text/raw bytes stay in source_revisions/CAS.
CREATE INDEX daily_capture_reply_targets ON relations(target_id,kind,source_id);
CREATE TABLE daily_capture_work (
 source_id TEXT PRIMARY KEY REFERENCES sources(id), identity TEXT NOT NULL CHECK(json_valid(identity)),
 config_id TEXT NOT NULL REFERENCES config_snapshots(id),
 state TEXT NOT NULL CHECK(state IN ('queued','collecting','retry','complete','blocked','failed')),
 cycle INTEGER NOT NULL DEFAULT 1, attempts INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL DEFAULT 0,
 checkpoint TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(checkpoint)),
 result TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(result)), reason TEXT,
 render_pending INTEGER NOT NULL DEFAULT 0, render_cursor INTEGER NOT NULL DEFAULT 0
) STRICT;
CREATE INDEX daily_capture_ready ON daily_capture_work(state,next_at,source_id);
CREATE INDEX daily_capture_render ON daily_capture_work(render_pending,source_id);
CREATE TABLE daily_capture_attempts (
 source_id TEXT NOT NULL REFERENCES daily_capture_work(source_id), cycle INTEGER NOT NULL, attempt INTEGER NOT NULL,
 generation INTEGER NOT NULL, config_id TEXT NOT NULL REFERENCES config_snapshots(id),
 started_at INTEGER NOT NULL, finished_at INTEGER, state TEXT NOT NULL, reason TEXT,
 checkpoint TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(checkpoint)),
 PRIMARY KEY(source_id,cycle,attempt)
) STRICT;
CREATE TRIGGER immutable_finished_capture_attempt BEFORE UPDATE ON daily_capture_attempts WHEN OLD.finished_at IS NOT NULL
 BEGIN SELECT RAISE(ABORT,'immutable finished capture attempt'); END;
CREATE TABLE daily_capture_origins (
 origin_id TEXT PRIMARY KEY REFERENCES activity_observations(id),
 source_id TEXT NOT NULL REFERENCES daily_capture_work(source_id)
) STRICT;
CREATE INDEX daily_capture_origin_source ON daily_capture_origins(source_id);
CREATE TRIGGER immutable_capture_origin BEFORE UPDATE ON daily_capture_origins
 BEGIN SELECT RAISE(ABORT,'immutable capture origin'); END;
CREATE TABLE daily_capture_cache (
 source_id TEXT PRIMARY KEY REFERENCES sources(id), observed_at INTEGER NOT NULL,
 observation_ids TEXT NOT NULL CHECK(json_valid(observation_ids))
) STRICT;
CREATE TABLE daily_capture_admission (singleton INTEGER PRIMARY KEY CHECK(singleton=1), last_rowid INTEGER NOT NULL) STRICT;
INSERT INTO daily_capture_admission VALUES(1,0);
CREATE TABLE daily_capture_lease (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), source_id TEXT REFERENCES daily_capture_work(source_id),
 generation INTEGER NOT NULL DEFAULT 0, lease_until INTEGER NOT NULL DEFAULT 0
) STRICT;
INSERT INTO daily_capture_lease(singleton) VALUES(1);
