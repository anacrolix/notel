create sequence seq_stream_id;

CREATE TABLE streams(
    stream_id integer primary key default nextval('seq_stream_id'),
    headers blob,
    start_datetime text not null default current_timestamp
);

CREATE TABLE events(
    stream_event_index integer,
    insert_timestamp timestamp default current_timestamp,
    payload blob not null,
    stream_id integer references streams(stream_id),
    primary key (stream_id, stream_event_index)
);