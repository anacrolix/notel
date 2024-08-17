insert into streams (
    start_timestamp,
    headers,
    stream_id
)
SELECT *
FROM read_json(
    'json_files/streams.*.json.zst', ("compression" = 'zstd'), (format = 'newline_delimited'), (maximum_depth = 0),
    ("columns" = main.struct_pack(start_datetime := 'timestamp', headers := 'json', stream_id := 'ubigint')));

insert into events (
    insert_datetime,
    payload,
    stream_id,
    stream_event_index)
select
    json->>'insert_datetime',
    json->>'payload',
    json->>'stream_id',
    json->>'stream_event_index'
from read_json(
      'json_files/events.*.json.zst',
      "compression" = 'zstd',
      format = 'newline_delimited',
      maximum_depth = 0,
      records = false);

--insert into events (
--    insert_datetime,
--    payload,
--    stream_id,
--    stream_event_index
--)
--select * from read_json(
--                  'json_files/events.*.json.zst',
--                  "compression" = 'zstd',
--                  format = 'newline_delimited',
--                  maximum_depth = 0,
--                  (columns = main.struct_pack(
--                      insert_datetime := 'timestamp',
--                      payload := 'json',
--                      stream_id := 'ubigint',
--                      stream_event_index := 'integer')));