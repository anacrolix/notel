CREATE VIEW streams AS SELECT *
FROM read_json(
    'json_files/streams.*.json.zst', ("compression" = 'zstd'), (format = 'newline_delimited'), (maximum_depth = 0),
    ("columns" = main.struct_pack(start_datetime := 'timestamp', headers := 'json', stream_id := 'ubigint')));


CREATE VIEW events AS SELECT *
FROM read_json(
    'json_files/events.*.json.zst',
    "compression" = 'zstd',
    format = 'newline_delimited',
    maximum_depth = 0,
    (columns = main.struct_pack(
        insert_datetime := 'timestamp',
        payload := 'json',
        stream_id := 'ubigint',
        stream_event_index := 'integer')));
