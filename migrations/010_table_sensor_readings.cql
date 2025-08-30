USE tcc;

CREATE TABLE IF NOT EXISTS sensor_readings (
    sensor_id text,
    day_bucket date,
    ts timeuuid,
    value double,
    unit text,
    tags map<text,text>,
    status smallint,
    PRIMARY KEY ((sensor_id, day_bucket), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
  'compaction_window_unit': 'DAYS',
    'compaction_window_size': '1'
  }
  -- sem TTL
  AND default_time_to_live = 0
  -- 1 dia
  AND gc_grace_seconds = 86400;