-- DA09 / #76. Immutable payloads reuse notion_documents and its semantic export.
CREATE TABLE reader_dirty (
 scope TEXT PRIMARY KEY, window_id TEXT, local_date TEXT NOT NULL, timezone TEXT NOT NULL,
 error TEXT
) STRICT;
CREATE TABLE reader_heads (
 scope TEXT PRIMARY KEY, window_id TEXT, local_date TEXT NOT NULL, timezone TEXT NOT NULL,
 manifest_id TEXT NOT NULL REFERENCES notion_documents(id)
) STRICT;
CREATE INDEX reader_heads_day ON reader_heads(local_date,timezone,scope);
CREATE TABLE reader_dependencies (
 scope TEXT NOT NULL, kind TEXT NOT NULL, entity TEXT NOT NULL,
 PRIMARY KEY(scope,kind,entity)
) STRICT;
CREATE INDEX reader_dependency_entity ON reader_dependencies(kind,entity,scope);
CREATE TABLE reader_activity_windows (
 window_id TEXT NOT NULL REFERENCES daily_windows(id), activity_id TEXT NOT NULL REFERENCES activities(id),
 PRIMARY KEY(window_id,activity_id)
) STRICT;
CREATE INDEX reader_activity_window_reverse ON reader_activity_windows(activity_id,window_id);
CREATE INDEX reader_activity_latest ON activity_observations(activity_id,json_extract(manifest,'$.discovery.provenance.window_id'),json_extract(manifest,'$.retrieved_at'),source_observation_id,id);
CREATE INDEX reader_observation_window ON activity_observations(json_extract(manifest,'$.discovery.provenance.window_id'),activity_id);
CREATE INDEX reader_day_windows ON daily_windows(local_date,timezone,id);
CREATE TABLE reader_bootstrap (name TEXT PRIMARY KEY, cursor TEXT NOT NULL DEFAULT '', done INTEGER NOT NULL DEFAULT 0) STRICT;
INSERT INTO reader_bootstrap(name) VALUES('windows'),('activities');
INSERT INTO reader_bootstrap(name,done) VALUES('refresh',1);
CREATE TABLE reader_settings (singleton INTEGER PRIMARY KEY CHECK(singleton=1), digest TEXT NOT NULL) STRICT;
INSERT INTO reader_settings VALUES(1,'');
CREATE TABLE reader_enrichments (
 source_id TEXT PRIMARY KEY REFERENCES sources(id), payload TEXT NOT NULL CHECK(json_valid(payload))
) STRICT;
CREATE TABLE reader_checkpoints (
 id TEXT PRIMARY KEY REFERENCES notion_documents(id), scope TEXT NOT NULL,
 manifest_id TEXT NOT NULL REFERENCES notion_documents(id), layer INTEGER NOT NULL CHECK(layer BETWEEN 1 AND 5),
 stale INTEGER NOT NULL DEFAULT 0 CHECK(stale IN (0,1))
) STRICT;
CREATE INDEX reader_checkpoint_scope ON reader_checkpoints(scope,layer,manifest_id);
CREATE INDEX reader_document_checkpoints ON notion_documents(json_extract(record,'$.data.manifest_id'),id) WHERE json_extract(record,'$.data.kind')='checkpoint';
-- Producer mutations enqueue constant-size keys in their existing transaction. The reader
-- drains each key through the dependency or activity reverse index in pages of64 scopes.
CREATE TABLE reader_dirty_inputs(kind TEXT NOT NULL,entity TEXT NOT NULL,cursor TEXT NOT NULL DEFAULT '',PRIMARY KEY(kind,entity)) STRICT;
CREATE TRIGGER reader_window_insert AFTER INSERT ON daily_windows BEGIN
 INSERT OR IGNORE INTO reader_dirty VALUES('window:'||NEW.id,NEW.id,NEW.local_date,NEW.timezone,NULL);
END;
CREATE TRIGGER reader_window_update AFTER UPDATE ON daily_windows
WHEN NEW.state IS NOT OLD.state OR NEW.cursor IS NOT OLD.cursor OR NEW.reason IS NOT OLD.reason OR NEW.attempts IS NOT OLD.attempts BEGIN
 INSERT OR IGNORE INTO reader_dirty VALUES('window:'||NEW.id,NEW.id,NEW.local_date,NEW.timezone,NULL);
END;
CREATE TRIGGER reader_activity_insert AFTER INSERT ON activity_observations BEGIN
 INSERT OR IGNORE INTO reader_activity_windows
 SELECT w.id,NEW.activity_id FROM daily_windows w WHERE w.id=json_extract(NEW.manifest,'$.discovery.provenance.window_id');
 INSERT INTO reader_dirty_inputs(kind,entity) VALUES('activity',NEW.activity_id) ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
CREATE TRIGGER reader_publication_update AFTER UPDATE ON notion_queue
WHEN NEW.state IS NOT OLD.state OR NEW.page_id IS NOT OLD.page_id OR NEW.receipt IS NOT OLD.receipt BEGIN
 INSERT INTO reader_dirty_inputs(kind,entity) VALUES('queue',NEW.id) ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
CREATE TRIGGER reader_source_insert AFTER INSERT ON observations BEGIN
 INSERT INTO reader_dirty_inputs(kind,entity) VALUES('source',NEW.source_id) ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
CREATE INDEX reader_failure_stream_lookup ON failure_streams(stream_key,fingerprint);
CREATE INDEX reader_report_fingerprint ON failure_reports(fingerprint,repository);
CREATE INDEX reader_pdf_observation ON notion_queue(observation_id,id) WHERE kind='pdf_attachment';
CREATE TRIGGER reader_capture_insert AFTER INSERT ON daily_capture_work BEGIN
 INSERT INTO reader_dirty_inputs(kind,entity) VALUES('capture',NEW.source_id) ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
CREATE TRIGGER reader_capture_update AFTER UPDATE ON daily_capture_work
WHEN NEW.state IS NOT OLD.state OR NEW.reason IS NOT OLD.reason OR NEW.cycle IS NOT OLD.cycle OR NEW.attempts IS NOT OLD.attempts BEGIN
 INSERT INTO reader_dirty_inputs(kind,entity) VALUES('capture',NEW.source_id) ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
CREATE TRIGGER reader_failure_stream_insert AFTER INSERT ON failure_streams BEGIN
 INSERT INTO reader_dirty_inputs(kind,entity) VALUES('report_stream',NEW.stream_key) ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
CREATE TRIGGER reader_report_insert AFTER INSERT ON failure_reports BEGIN
 INSERT INTO reader_dirty_inputs(kind,entity) VALUES('report',NEW.fingerprint) ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
CREATE TRIGGER reader_report_update AFTER UPDATE ON failure_reports
WHEN NEW.issue_url IS NOT OLD.issue_url OR NEW.notified_sequence IS NOT OLD.notified_sequence BEGIN
 INSERT INTO reader_dirty_inputs(kind,entity) VALUES('report',NEW.fingerprint) ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
CREATE TRIGGER reader_pdf_insert AFTER INSERT ON notion_pdf BEGIN
 INSERT INTO reader_dirty_inputs(kind,entity) SELECT 'pdf_observation',CAST(observation_id AS TEXT) FROM notion_queue WHERE id=NEW.queue_id AND observation_id IS NOT NULL
 ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
CREATE TRIGGER reader_pdf_update AFTER UPDATE ON notion_pdf
WHEN NEW.upload_state IS NOT OLD.upload_state OR NEW.attachment_state IS NOT OLD.attachment_state OR NEW.verified_sha256 IS NOT OLD.verified_sha256 OR NEW.block_id IS NOT OLD.block_id OR NEW.reason IS NOT OLD.reason BEGIN
 INSERT INTO reader_dirty_inputs(kind,entity) SELECT 'pdf_observation',CAST(observation_id AS TEXT) FROM notion_queue WHERE id=NEW.queue_id AND observation_id IS NOT NULL
 ON CONFLICT(kind,entity) DO UPDATE SET cursor='';
END;
