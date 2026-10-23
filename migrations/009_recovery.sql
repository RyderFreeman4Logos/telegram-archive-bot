CREATE TABLE operational_holds (name TEXT PRIMARY KEY, reason TEXT NOT NULL) STRICT;
CREATE TABLE semantic_derivatives (
 derivation_key TEXT PRIMARY KEY, kind TEXT NOT NULL,
 manifest TEXT NOT NULL CHECK(json_valid(manifest)),
 text_digest TEXT REFERENCES blobs(digest),
 cache_eligible INTEGER NOT NULL DEFAULT 0 CHECK(cache_eligible=0)
) STRICT;
CREATE TABLE semantic_unavailable (
 kind TEXT NOT NULL, entity_id TEXT NOT NULL,
 manifest TEXT NOT NULL CHECK(json_valid(manifest)),
 reason TEXT NOT NULL, PRIMARY KEY(kind,entity_id)
) STRICT;
