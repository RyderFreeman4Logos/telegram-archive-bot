CREATE TABLE message_origins (
 bot_id INTEGER NOT NULL, update_id INTEGER NOT NULL,
 message_id INTEGER NOT NULL CHECK(message_id>0),
 PRIMARY KEY(bot_id,update_id),
 FOREIGN KEY(bot_id,update_id) REFERENCES inbox(bot_id,update_id)
) STRICT;
