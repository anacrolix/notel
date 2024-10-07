-- This is the schema for Postgres.
CREATE TABLE IF NOT EXISTS streams(
  stream_id SERIAL PRIMARY KEY,
  headers JSONB NOT NULL,
  start_datetime TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(
  stream_event_index INTEGER NOT NULL,
  insert_datetime TIMESTAMP NOT NULL,
  payload JSONB NOT NULL,
  stream_id INTEGER REFERENCES streams(stream_id) NOT NULL);
