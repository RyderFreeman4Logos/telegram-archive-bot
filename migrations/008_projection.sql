ALTER TABLE projection_events ADD COLUMN group_key TEXT;
CREATE TABLE projection_intents (
 name TEXT PRIMARY KEY, intent TEXT NOT NULL CHECK(json_valid(intent))
) STRICT;
CREATE TABLE projection_pushes (
 name TEXT NOT NULL, target TEXT NOT NULL, commit_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','confirmed','uncertain','rejected')),
 PRIMARY KEY(name,target)
) STRICT;
CREATE TRIGGER export_alias AFTER INSERT ON source_aliases BEGIN
 INSERT INTO projection_events(kind,entity_id) VALUES('alias',json_array(NEW.source_id,NEW.url,NEW.method)) ON CONFLICT DO NOTHING;
END;
INSERT INTO projection_events(kind,entity_id)
 SELECT 'alias',json_array(source_id,url,method) FROM source_aliases ORDER BY source_id,url;
CREATE TABLE archive_identity (id TEXT PRIMARY KEY CHECK(length(id)=64)) STRICT;
INSERT INTO archive_identity VALUES(lower(hex(randomblob(32))));
