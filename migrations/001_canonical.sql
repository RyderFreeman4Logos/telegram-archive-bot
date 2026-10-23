CREATE TABLE blobs (
 digest TEXT PRIMARY KEY CHECK(length(digest)=64), size INTEGER NOT NULL CHECK(size>=0),
 created_at INTEGER NOT NULL
) STRICT;
CREATE TABLE sources (
 id TEXT PRIMARY KEY, provider TEXT NOT NULL, external_id TEXT NOT NULL,
 current_revision TEXT REFERENCES source_revisions(id), UNIQUE(provider,external_id)
) STRICT;
CREATE TABLE source_aliases (
 source_id TEXT NOT NULL REFERENCES sources(id), url TEXT NOT NULL,
 method TEXT NOT NULL, PRIMARY KEY(source_id,url)
) STRICT;
CREATE TABLE source_revisions (
 id TEXT PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(id),
 content_digest TEXT NOT NULL, body TEXT NOT NULL, metadata TEXT NOT NULL CHECK(json_valid(metadata)),
 completeness TEXT NOT NULL CHECK(completeness IN ('partial','complete')),
 UNIQUE(source_id,content_digest), UNIQUE(id,source_id)
) STRICT;
CREATE TRIGGER current_revision_owner BEFORE UPDATE OF current_revision ON sources
WHEN NEW.current_revision IS NOT NULL AND NOT EXISTS (
 SELECT 1 FROM source_revisions WHERE id=NEW.current_revision AND source_id=NEW.id
) BEGIN SELECT RAISE(ABORT,'revision belongs to another source'); END;
CREATE TRIGGER immutable_revision BEFORE UPDATE ON source_revisions
BEGIN SELECT RAISE(ABORT,'immutable revision'); END;
CREATE TABLE observations (
 id INTEGER PRIMARY KEY, source_id TEXT NOT NULL REFERENCES sources(id),
 revision_id TEXT, observed_at INTEGER NOT NULL, status TEXT NOT NULL,
 extractor TEXT NOT NULL, evidence TEXT NOT NULL CHECK(json_valid(evidence)),
 raw_digest TEXT REFERENCES blobs(digest),
 FOREIGN KEY(revision_id,source_id) REFERENCES source_revisions(id,source_id)
) STRICT;
CREATE TABLE relations (
 source_id TEXT NOT NULL REFERENCES sources(id), target_id TEXT NOT NULL REFERENCES sources(id),
 kind TEXT NOT NULL CHECK(kind IN ('quotes','replies_to','retweets','links','edit_of')),
 observation_id INTEGER NOT NULL REFERENCES observations(id),
 PRIMARY KEY(source_id,target_id,kind,observation_id)
) STRICT;
CREATE TABLE views (
 id TEXT PRIMARY KEY, root_id TEXT NOT NULL REFERENCES sources(id),
 manifest TEXT NOT NULL CHECK(json_valid(manifest))
) STRICT;
CREATE TABLE view_members (
 view_id TEXT NOT NULL REFERENCES views(id), ordinal INTEGER NOT NULL CHECK(ordinal>=0),
 source_id TEXT NOT NULL REFERENCES sources(id), revision_id TEXT,
 missing_reason TEXT, PRIMARY KEY(view_id,ordinal), UNIQUE(view_id,source_id),
 CHECK((revision_id IS NULL)=(missing_reason IS NOT NULL)),
 FOREIGN KEY(revision_id,source_id) REFERENCES source_revisions(id,source_id)
) STRICT;
CREATE TABLE config_snapshots (
 id TEXT PRIMARY KEY, config TEXT NOT NULL CHECK(json_valid(config)),
 tools TEXT NOT NULL CHECK(json_valid(tools))
) STRICT;
CREATE TABLE artifacts (
 id TEXT PRIMARY KEY, input_id TEXT NOT NULL, profile TEXT NOT NULL,
 blob_digest TEXT NOT NULL REFERENCES blobs(digest),
 validation TEXT NOT NULL CHECK(json_valid(validation)),
 manifest TEXT NOT NULL CHECK(json_valid(manifest))
) STRICT;
CREATE TABLE inbox (
 bot_id INTEGER NOT NULL, update_id INTEGER NOT NULL, chat_id INTEGER NOT NULL,
 user_id INTEGER NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload)),
 PRIMARY KEY(bot_id,update_id)
) STRICT;
CREATE TABLE polling_cursors (bot_id INTEGER PRIMARY KEY, next_offset INTEGER NOT NULL) STRICT;
CREATE TABLE jobs (
 id TEXT PRIMARY KEY, bot_id INTEGER NOT NULL, update_id INTEGER NOT NULL,
 ordinal INTEGER NOT NULL, request TEXT NOT NULL CHECK(json_valid(request)),
 config_id TEXT NOT NULL REFERENCES config_snapshots(id),
 state TEXT NOT NULL CHECK(state IN ('queued','running','retryable','complete','failed','cancelled','paused')),
 phase TEXT NOT NULL DEFAULT 'fetch', attempt INTEGER NOT NULL DEFAULT 0,
 generation INTEGER NOT NULL DEFAULT 0, lease_until INTEGER, deadline INTEGER,
 available_at INTEGER NOT NULL DEFAULT 0, cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0,1)),
 result TEXT CHECK(result IS NULL OR json_valid(result)), error TEXT,
 UNIQUE(bot_id,update_id,ordinal), FOREIGN KEY(bot_id,update_id) REFERENCES inbox(bot_id,update_id)
) STRICT;
CREATE TABLE phase_outputs (
 job_id TEXT NOT NULL REFERENCES jobs(id), phase TEXT NOT NULL,
 output TEXT NOT NULL CHECK(json_valid(output)), PRIMARY KEY(job_id,phase)
) STRICT;
CREATE TABLE outbox (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id),
 kind TEXT NOT NULL, payload TEXT NOT NULL CHECK(json_valid(payload)),
 state TEXT NOT NULL CHECK(state IN ('pending','sending','confirmed','retryable','terminal','unknown','paused')),
 attempt INTEGER NOT NULL DEFAULT 0, available_at INTEGER NOT NULL DEFAULT 0,
 receipt TEXT CHECK(receipt IS NULL OR json_valid(receipt)), error TEXT
) STRICT;
CREATE TABLE projection_events (
 id INTEGER PRIMARY KEY, kind TEXT NOT NULL, entity_id TEXT NOT NULL,
 UNIQUE(kind,entity_id)
) STRICT;
CREATE TABLE projection_cursors (
 name TEXT PRIMARY KEY, event_id INTEGER NOT NULL DEFAULT 0,
 local_commit TEXT, pushed_commit TEXT
) STRICT;
CREATE INDEX jobs_available ON jobs(state,available_at);
CREATE INDEX outbox_available ON outbox(state,available_at);
