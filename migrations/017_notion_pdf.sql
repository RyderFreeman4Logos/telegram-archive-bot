-- DA07 / #74. Attachment work is an extension of the DA06 durable outbox.
CREATE TABLE notion_pdf (
 queue_id TEXT PRIMARY KEY REFERENCES notion_queue(id),
 upload_state TEXT NOT NULL DEFAULT 'pending', attachment_state TEXT NOT NULL DEFAULT 'pending',
 upload_id TEXT, upload_expiry INTEGER, block_id TEXT, status_block_id TEXT,
 status_dispatched INTEGER NOT NULL DEFAULT 0, attachment_dispatched INTEGER NOT NULL DEFAULT 0,
 read_only INTEGER NOT NULL DEFAULT 0 CHECK(read_only IN (0,1)),
 verified_sha256 TEXT, verified_at INTEGER, reason TEXT
) STRICT;

-- Landing provenance lookup is bounded by indexed target identity.
CREATE INDEX relations_pdf_target ON relations(target_id,kind,source_id);

-- One bounded pass admits pre-017 already-published PDF observation pages.
CREATE TABLE notion_pdf_bootstrap(singleton INTEGER PRIMARY KEY CHECK(singleton=1),cursor TEXT NOT NULL DEFAULT '',done INTEGER NOT NULL DEFAULT 0 CHECK(done IN (0,1))) STRICT;
INSERT INTO notion_pdf_bootstrap(singleton) VALUES(1);
CREATE INDEX notion_pdf_admission ON notion_queue(kind,id) WHERE state='complete';
