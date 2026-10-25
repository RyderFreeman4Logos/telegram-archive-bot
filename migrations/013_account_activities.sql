-- DA01 / #68. Source bodies remain solely in source_revisions.
CREATE TABLE accounts (
    id TEXT PRIMARY KEY,
    manifest TEXT NOT NULL CHECK(json_valid(manifest))
) STRICT;
CREATE TABLE account_observations (
    id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL REFERENCES accounts(id),
    manifest TEXT NOT NULL CHECK(json_valid(manifest))
) STRICT;
CREATE INDEX account_observations_account ON account_observations(account_id);
CREATE TABLE discovery_runs (
    id TEXT PRIMARY KEY,
    manifest TEXT NOT NULL CHECK(json_valid(manifest))
) STRICT;
CREATE TABLE activities (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES accounts(id),
    source_id TEXT NOT NULL REFERENCES sources(id),
    kind TEXT NOT NULL CHECK(kind IN ('original','quote','reply','repost')),
    manifest TEXT NOT NULL CHECK(json_valid(manifest)),
    UNIQUE(id, source_id)
) STRICT;
CREATE INDEX activities_actor ON activities(actor_id);
CREATE INDEX activities_source ON activities(source_id);
CREATE TABLE activity_observations (
    id TEXT PRIMARY KEY,
    activity_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_observation_id INTEGER NOT NULL REFERENCES observations(id),
    account_observation_id TEXT NOT NULL REFERENCES account_observations(id),
    discovery_run_id TEXT NOT NULL REFERENCES discovery_runs(id),
    view_id TEXT NOT NULL REFERENCES views(id),
    manifest TEXT NOT NULL CHECK(json_valid(manifest)),
    FOREIGN KEY(activity_id,source_id) REFERENCES activities(id,source_id)
) STRICT;
CREATE INDEX activity_observations_activity ON activity_observations(activity_id);
CREATE INDEX activity_observations_discovery ON activity_observations(discovery_run_id);
CREATE TRIGGER immutable_account BEFORE UPDATE ON accounts BEGIN SELECT RAISE(ABORT,'immutable account'); END;
CREATE TRIGGER immutable_account_observation BEFORE UPDATE ON account_observations BEGIN SELECT RAISE(ABORT,'immutable account observation'); END;
CREATE TRIGGER immutable_discovery_run BEFORE UPDATE ON discovery_runs BEGIN SELECT RAISE(ABORT,'immutable discovery run'); END;
CREATE TRIGGER immutable_activity BEFORE UPDATE ON activities BEGIN SELECT RAISE(ABORT,'immutable activity'); END;
CREATE TRIGGER immutable_activity_observation BEFORE UPDATE ON activity_observations BEGIN SELECT RAISE(ABORT,'immutable activity observation'); END;
