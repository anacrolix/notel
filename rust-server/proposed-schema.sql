-- Payload is what the application sends, collector is what the server has added.
CREATE TABLE streams(stream_id integer primary key, headers blob, start_datetime text not null) strict;
CREATE TABLE events(insert_datetime text, payload blob, stream_id integer references streams(stream_id)) strict;
-- This is just an example of how you can do indexes on JSON. The user could do it for their own
-- payloads and query patterns.
CREATE INDEX event_types on events(payload->'type');