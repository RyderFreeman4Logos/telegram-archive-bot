-- DA08 / #75. Operational private accounting; never part of semantic Git export.
-- Parent integration must apply the independently owned 016 and 017 first.
CREATE TABLE failure_incidents (
 id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL UNIQUE, metadata TEXT NOT NULL CHECK(json_valid(metadata)),
 occurrences INTEGER NOT NULL DEFAULT 0, consecutive INTEGER NOT NULL DEFAULT 0,
 first_at INTEGER NOT NULL, last_at INTEGER NOT NULL, active_since INTEGER,
 sequence INTEGER NOT NULL DEFAULT 0, actionable INTEGER NOT NULL CHECK(actionable IN (0,1)),
 state TEXT NOT NULL CHECK(state IN ('active','recovered'))
) STRICT;
CREATE TABLE failure_events (
 event_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL REFERENCES failure_incidents(fingerprint),
 observed_at INTEGER NOT NULL, sequence INTEGER NOT NULL, outcome TEXT NOT NULL, window_key TEXT,
 private_record TEXT NOT NULL CHECK(json_valid(private_record)),
 public_sample TEXT NOT NULL CHECK(json_valid(public_sample))
) STRICT;
CREATE INDEX failure_events_incident ON failure_events(fingerprint,observed_at DESC,event_id);
CREATE INDEX failure_events_window ON failure_events(fingerprint,window_key);
CREATE INDEX failure_events_sequence ON failure_events(fingerprint,sequence);
CREATE TABLE failure_streams (
 fingerprint TEXT NOT NULL REFERENCES failure_incidents(fingerprint), stream_key TEXT NOT NULL,
 consecutive INTEGER NOT NULL, active_since INTEGER,
 PRIMARY KEY(fingerprint,stream_key)
) STRICT;
CREATE INDEX failure_streams_active ON failure_streams(fingerprint,active_since) WHERE consecutive>0;
CREATE INDEX failure_streams_recover ON failure_streams(stream_key,fingerprint) WHERE consecutive>0;
CREATE TABLE failure_report_repositories (
 repository TEXT PRIMARY KEY, cursor INTEGER NOT NULL DEFAULT 0
) STRICT;
CREATE TABLE failure_reporting_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1),restore_through INTEGER NOT NULL DEFAULT 0) STRICT;
INSERT INTO failure_reporting_state(singleton) VALUES(1);
CREATE TABLE failure_reports (
 repository TEXT NOT NULL REFERENCES failure_report_repositories(repository),
 fingerprint TEXT NOT NULL REFERENCES failure_incidents(fingerprint),
 state TEXT NOT NULL DEFAULT 'pending_configuration', next_at INTEGER NOT NULL DEFAULT 0,
 issue_number INTEGER, issue_url TEXT, notified_sequence INTEGER NOT NULL DEFAULT 0,
 last_notified INTEGER NOT NULL DEFAULT 0, action TEXT CHECK(action IN ('create','comment','reopen')),
 pending_body TEXT, pending_sequence INTEGER, uncertain INTEGER NOT NULL DEFAULT 0,
 attempt INTEGER NOT NULL DEFAULT 0, error TEXT,
 PRIMARY KEY(repository,fingerprint)
) STRICT;
CREATE INDEX failure_reports_due ON failure_reports(repository,next_at,fingerprint) WHERE state != 'idle';
CREATE TRIGGER failure_report_dirty AFTER UPDATE OF sequence ON failure_incidents BEGIN
 UPDATE failure_reports SET state=CASE WHEN action IS NULL THEN 'pending' ELSE state END WHERE fingerprint=NEW.fingerprint;
END;
CREATE TABLE failure_report_attempts (
 repository TEXT NOT NULL, fingerprint TEXT NOT NULL, number INTEGER NOT NULL,
 action TEXT NOT NULL, body_digest TEXT NOT NULL, started_at INTEGER NOT NULL,
 finished_at INTEGER, outcome TEXT, verified_at INTEGER,
 PRIMARY KEY(repository,fingerprint,number),
 FOREIGN KEY(repository,fingerprint) REFERENCES failure_reports(repository,fingerprint)
) STRICT;
CREATE INDEX failure_report_attempts_unfinished ON failure_report_attempts(repository,fingerprint) WHERE finished_at IS NULL;
CREATE TRIGGER immutable_failure_report_target BEFORE UPDATE OF repository,fingerprint ON failure_reports
BEGIN SELECT RAISE(ABORT,'immutable failure report target'); END;
CREATE TRIGGER immutable_failure_event BEFORE UPDATE ON failure_events
BEGIN SELECT RAISE(ABORT,'immutable failure event'); END;
