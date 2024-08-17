create table streams (
    stream_id ubigint primary key,
    start_timestamp timestamp,
    headers json,
);

CREATE TABLE events(
    stream_id ubigint references streams(stream_id),
    stream_event_index integer,
    insert_datetime TIMESTAMP,
    payload JSON,
    unique (stream_id, stream_event_index)
);
