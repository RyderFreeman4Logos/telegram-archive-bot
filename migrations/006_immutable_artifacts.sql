CREATE TRIGGER immutable_artifact BEFORE UPDATE ON artifacts
BEGIN SELECT RAISE(ABORT,'immutable validated artifact'); END;
