-- DA06 / #73, #98: due work is admitted in immutable creation order.
-- Retain the available_at-leading index for its existing callers. This partial
-- index excludes terminal/owned work and avoids sorting the active backlog.
CREATE INDEX notion_queue_due_created ON notion_queue(destination_key,created_at,id)
 WHERE state NOT IN ('complete','conflict','creating');
