-- DA06 / #73. This outbox is independent of Telegram/Git receipts.
CREATE TABLE notion_binding (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 destination_key TEXT, destination TEXT CHECK(destination IS NULL OR json_valid(destination)),
 bootstrap_reconcile INTEGER NOT NULL DEFAULT 0 CHECK(bootstrap_reconcile IN (0,1))
) STRICT;
INSERT INTO notion_binding(singleton) VALUES(1);
CREATE TABLE notion_queue (
 id TEXT PRIMARY KEY, kind TEXT NOT NULL, entity_id TEXT NOT NULL,
 observation_id INTEGER REFERENCES observations(id), created_at INTEGER NOT NULL,
 destination_key TEXT, destination TEXT CHECK(destination IS NULL OR json_valid(destination)),
 publication_key TEXT, record TEXT CHECK(record IS NULL OR json_valid(record)), expected_digest TEXT,
 state TEXT NOT NULL DEFAULT 'pending_configuration' CHECK(state IN
 ('pending_configuration','ready','creating','reconcile','retry_wait','blocked_auth','conflict','complete')),
 may_have_remote INTEGER NOT NULL DEFAULT 0 CHECK(may_have_remote IN (0,1)),
 attempts INTEGER NOT NULL DEFAULT 0, create_attempts INTEGER NOT NULL DEFAULT 0,
 available_at INTEGER NOT NULL DEFAULT 0, page_id TEXT, candidate_page_id TEXT,
 progress TEXT CHECK(progress IS NULL OR json_valid(progress)),
 receipt TEXT CHECK(receipt IS NULL OR json_valid(receipt)), error TEXT,
 UNIQUE(kind,entity_id), UNIQUE(destination_key,publication_key)
) STRICT;
CREATE INDEX notion_queue_available ON notion_queue(state,available_at,created_at,id);
CREATE INDEX notion_queue_destination ON notion_queue(destination_key,state);
CREATE TRIGGER immutable_notion_intent BEFORE UPDATE ON notion_queue
WHEN NEW.id IS NOT OLD.id OR NEW.kind IS NOT OLD.kind OR NEW.entity_id IS NOT OLD.entity_id
 OR NEW.observation_id IS NOT OLD.observation_id OR NEW.created_at IS NOT OLD.created_at
 OR (OLD.destination_key IS NOT NULL AND
    (NEW.destination_key IS NOT OLD.destination_key OR NEW.destination IS NOT OLD.destination))
 OR (OLD.record IS NOT NULL AND
    (NEW.record IS NOT OLD.record OR NEW.publication_key IS NOT OLD.publication_key OR NEW.expected_digest IS NOT OLD.expected_digest))
BEGIN SELECT RAISE(ABORT,'immutable Notion intent'); END;
CREATE TABLE notion_attempts (
 queue_id TEXT NOT NULL REFERENCES notion_queue(id), number INTEGER NOT NULL,
 action TEXT NOT NULL CHECK(action IN ('create','reconcile')),
 started_at INTEGER NOT NULL, finished_at INTEGER, outcome TEXT,
 page_id TEXT, progress TEXT CHECK(progress IS NULL OR json_valid(progress)),
 receipt TEXT CHECK(receipt IS NULL OR json_valid(receipt)),
 PRIMARY KEY(queue_id,number)
) STRICT;
CREATE TABLE notion_bootstrap (name TEXT PRIMARY KEY, cursor TEXT NOT NULL DEFAULT '', done INTEGER NOT NULL DEFAULT 0) STRICT;
CREATE TABLE notion_operator_actions (
 id INTEGER PRIMARY KEY, queue_id TEXT NOT NULL REFERENCES notion_queue(id),
 action TEXT NOT NULL CHECK(action IN ('retry','reconcile')), created_at INTEGER NOT NULL,
 candidate_page_id TEXT
) STRICT;
INSERT INTO notion_bootstrap(name) VALUES('observations'),('account_observations'),('activity_observations'),('notion_documents');
-- Derived publication records are semantic content, separate from source records and operational receipts.
CREATE TABLE notion_documents (id TEXT PRIMARY KEY, record TEXT NOT NULL CHECK(json_valid(record))) STRICT;
CREATE TRIGGER immutable_notion_document BEFORE UPDATE ON notion_documents
BEGIN SELECT RAISE(ABORT,'immutable Notion document'); END;
CREATE TRIGGER queue_notion_observation AFTER INSERT ON observations BEGIN
 INSERT OR IGNORE INTO notion_queue(id,kind,entity_id,observation_id,created_at,destination_key,destination,state,may_have_remote)
 SELECT 'observation:'||NEW.id,'observation',CAST(NEW.id AS TEXT),NEW.id,NEW.observed_at,destination_key,destination,
 CASE WHEN EXISTS(SELECT 1 FROM operational_holds) THEN 'reconcile' ELSE 'pending_configuration' END,
 EXISTS(SELECT 1 FROM operational_holds) FROM notion_binding;
 INSERT OR IGNORE INTO notion_queue(id,kind,entity_id,observation_id,created_at,destination_key,destination,state,may_have_remote)
 SELECT 'revision:'||NEW.revision_id,'revision',NEW.revision_id,NEW.id,NEW.observed_at,destination_key,destination,
 CASE WHEN EXISTS(SELECT 1 FROM operational_holds) THEN 'reconcile' ELSE 'pending_configuration' END,
 EXISTS(SELECT 1 FROM operational_holds) FROM notion_binding WHERE NEW.revision_id IS NOT NULL;
END;
CREATE TRIGGER queue_notion_account AFTER INSERT ON account_observations BEGIN
 INSERT OR IGNORE INTO notion_queue(id,kind,entity_id,created_at,destination_key,destination,state,may_have_remote)
 SELECT 'account:'||NEW.id,'account',NEW.id,json_extract(NEW.manifest,'$.retrieved_at'),destination_key,destination,
 CASE WHEN EXISTS(SELECT 1 FROM operational_holds) THEN 'reconcile' ELSE 'pending_configuration' END,
 EXISTS(SELECT 1 FROM operational_holds) FROM notion_binding;
END;
CREATE TRIGGER queue_notion_activity AFTER INSERT ON activity_observations BEGIN
 INSERT OR IGNORE INTO notion_queue(id,kind,entity_id,observation_id,created_at,destination_key,destination,state,may_have_remote)
 SELECT 'activity:'||NEW.id,'activity',NEW.id,NEW.source_observation_id,json_extract(NEW.manifest,'$.retrieved_at'),destination_key,destination,
 CASE WHEN EXISTS(SELECT 1 FROM operational_holds) THEN 'reconcile' ELSE 'pending_configuration' END,
 EXISTS(SELECT 1 FROM operational_holds) FROM notion_binding;
END;
CREATE TRIGGER queue_notion_document AFTER INSERT ON notion_documents BEGIN
 INSERT OR IGNORE INTO notion_queue(id,kind,entity_id,created_at,destination_key,destination,state,may_have_remote)
 SELECT 'document:'||NEW.id,'document',NEW.id,unixepoch(),destination_key,destination,
 CASE WHEN EXISTS(SELECT 1 FROM operational_holds) THEN 'reconcile' ELSE 'pending_configuration' END,
 EXISTS(SELECT 1 FROM operational_holds) FROM notion_binding;
 INSERT OR IGNORE INTO projection_events(kind,entity_id) VALUES('notion_document',NEW.id);
END;

-- Keep each polling tick independent of completed publication history.
CREATE INDEX notion_attempts_unfinished ON notion_attempts(queue_id,number) WHERE finished_at IS NULL;
CREATE INDEX notion_queue_due ON notion_queue(destination_key,available_at,created_at,id) WHERE state NOT IN ('complete','conflict','creating');
CREATE INDEX notion_queue_unbound ON notion_queue(destination_key,created_at,id) WHERE destination_key IS NULL;
