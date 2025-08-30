USE tcc;

CREATE TABLE IF NOT EXISTS sensor_last_reading (
    sensor_id text PRIMARY KEY,
    ts timeuuid,
    value double,
    unit text,
    status smallint,
    tags map<text,text>
);