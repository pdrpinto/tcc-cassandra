USE tcc;

CREATE TABLE IF NOT EXISTS sensor_hourly_agg (
    sensor_id text,
    hour_bucket timestamp,
    cnt counter,
    sum counter,
    PRIMARY KEY ((sensor_id), hour_bucket)
) WITH compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': '1'
};