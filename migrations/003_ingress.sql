CREATE TABLE ingress_rejections (
 bot_id INTEGER NOT NULL, update_id INTEGER NOT NULL, reason TEXT NOT NULL,
 PRIMARY KEY(bot_id,update_id)
) STRICT;
