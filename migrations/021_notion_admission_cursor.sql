-- DA06 / #73, #102. Scheduling only: historical table values are unchanged.
CREATE TABLE notion_admission_cursor (
 destination_key TEXT PRIMARY KEY,
 delayed_next INTEGER NOT NULL CHECK(delayed_next IN (0,1))
) STRICT;

-- Both lanes use notion_queue_due; global creation-order selection is retired.
DROP INDEX notion_queue_due_created;
