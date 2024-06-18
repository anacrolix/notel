-- Payload is what the application sends, collector is what the server has added.
CREATE TABLE data(payload blob, collector blob) strict;
-- This is just an example of how you can do indexes on JSON. The user could do it for their own
-- payloads and query patterns.
CREATE INDEX type on data(payload->'type');