-- DA02 / #69: operational planning; full backups retain this state.
CREATE TABLE daily_watch_bindings (
 watch_key TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES accounts(id),
 identity TEXT NOT NULL CHECK(json_valid(identity)), enabled INTEGER NOT NULL CHECK(enabled IN (0,1))
) STRICT;
CREATE INDEX daily_enabled_accounts ON daily_watch_bindings(enabled,account_id);
CREATE TABLE daily_account_resolutions (
 watch_key TEXT PRIMARY KEY REFERENCES daily_watch_bindings(watch_key),
 account_id TEXT NOT NULL REFERENCES accounts(id), identity TEXT NOT NULL CHECK(json_valid(identity)),
 bound_at INTEGER NOT NULL, evidence TEXT NOT NULL CHECK(json_valid(evidence))
) STRICT;
CREATE TRIGGER immutable_daily_resolution BEFORE UPDATE ON daily_account_resolutions
 BEGIN SELECT RAISE(ABORT,'immutable daily account resolution'); END;
CREATE TABLE daily_windows (
 id TEXT PRIMARY KEY, account_id TEXT NOT NULL REFERENCES accounts(id),
 timezone TEXT NOT NULL, local_date TEXT NOT NULL, utc_start INTEGER NOT NULL, utc_end INTEGER NOT NULL CHECK(utc_end>utc_start),
 config_id TEXT NOT NULL REFERENCES config_snapshots(id), manifest TEXT NOT NULL CHECK(json_valid(manifest)),
 state TEXT NOT NULL CHECK(state IN ('planned','collecting','partial','blocked','failed','complete','open_current_day')),
 attempts INTEGER NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0,
 lease_until INTEGER, cursor TEXT, reason TEXT, next_attempt_at INTEGER NOT NULL DEFAULT 0,
 UNIQUE(account_id,timezone,local_date)
) STRICT;
CREATE INDEX daily_ready_windows ON daily_windows(state,next_attempt_at,utc_start,id);
CREATE TRIGGER immutable_daily_plan BEFORE UPDATE OF id,account_id,timezone,local_date,utc_start,utc_end,config_id,manifest ON daily_windows
 BEGIN SELECT RAISE(ABORT,'immutable daily plan'); END;
CREATE TABLE daily_attempts (
 window_id TEXT NOT NULL REFERENCES daily_windows(id), attempt INTEGER NOT NULL,
 generation INTEGER NOT NULL, config_id TEXT NOT NULL REFERENCES config_snapshots(id),
 watch TEXT NOT NULL CHECK(json_valid(watch)), started_at INTEGER NOT NULL, finished_at INTEGER,
 state TEXT NOT NULL, cursor TEXT, pages INTEGER NOT NULL DEFAULT 0,
 evidence TEXT NOT NULL CHECK(json_valid(evidence)),
 PRIMARY KEY(window_id,attempt)
) STRICT;
CREATE TABLE daily_pages (
 window_id TEXT NOT NULL, attempt INTEGER NOT NULL, page INTEGER NOT NULL,
 cursor TEXT NOT NULL, evidence TEXT NOT NULL CHECK(json_valid(evidence)),
 PRIMARY KEY(window_id,attempt,page), UNIQUE(window_id,attempt,cursor),
 FOREIGN KEY(window_id,attempt) REFERENCES daily_attempts(window_id,attempt)
) STRICT;
CREATE TRIGGER immutable_daily_page BEFORE UPDATE ON daily_pages
 BEGIN SELECT RAISE(ABORT,'immutable daily page evidence'); END;
CREATE TABLE daily_admission_cursors (
 account_id TEXT NOT NULL REFERENCES accounts(id), timezone TEXT NOT NULL, next_date TEXT NOT NULL, reconcile_date TEXT,
 PRIMARY KEY(account_id,timezone)
) STRICT;
CREATE TABLE daily_scheduler (singleton INTEGER PRIMARY KEY CHECK(singleton=1), last_account TEXT) STRICT;
INSERT INTO daily_scheduler VALUES(1,NULL);
