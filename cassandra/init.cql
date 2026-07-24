

CREATE KEYSPACE IF NOT EXISTS tfl
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};



CREATE TABLE IF NOT EXISTS tfl.bronze_arrivals (
    ingest_hour     text,       -- 'YYYY-MM-DDTHH'
    kafka_partition int,
    kafka_offset    bigint,
    kafka_ts        timestamp,
    kafka_key       text,
    raw_payload     text,
    PRIMARY KEY ((ingest_hour, kafka_partition), kafka_offset)
) WITH CLUSTERING ORDER BY (kafka_offset ASC);



CREATE TABLE IF NOT EXISTS tfl.silver_arrivals (
    line_id          text,
    arrival_date     date,
    expected_arrival timestamp,
    vehicle_id       text,
    naptan_id        text,
    line_name        text,
    station_name     text,
    platform_name    text,
    direction        text,   -- TfL operational: inbound / outbound / unknown
    compass_direction text,  -- derived from platform_name: northbound / ...
    destination_name text,
    towards          text,
    current_location text,
    time_to_station  int,
    prediction_ts    timestamp,
    mode_name        text,
    source_sent_ts     timestamp,   -- when TfL emitted the prediction
    source_lag_seconds int,         -- TfL emit -> Kafka landing, in seconds
    processed_at     timestamp,
    PRIMARY KEY ((line_id, arrival_date), expected_arrival, vehicle_id, naptan_id)
) WITH CLUSTERING ORDER BY (expected_arrival DESC);



CREATE TABLE IF NOT EXISTS tfl.quarantine_arrivals (
    ingest_hour     text,
    error_type      text,
    kafka_partition int,
    kafka_offset    bigint,
    raw_payload     text,
    quarantined_at  timestamp,
    PRIMARY KEY ((ingest_hour), error_type, kafka_partition, kafka_offset)
);



CREATE TABLE IF NOT EXISTS tfl.gold_line_health (
    line_id                 text,
    window_start            timestamp,
    window_end              timestamp,
    line_name               text,
    predictions             bigint,
    active_vehicles         bigint,
    stations_covered        bigint,
    avg_time_to_station     double,
    min_time_to_station     int,
    max_time_to_station     int,
    stddev_time_to_station  double,
    avg_source_lag_seconds  double,
    max_source_lag_seconds  int,
    computed_at             timestamp,
    PRIMARY KEY ((line_id), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);



CREATE TABLE IF NOT EXISTS tfl.pipeline_metrics (
    job_name         text,
    metric_date      date,
    batch_id         bigint,
    batch_ts         timestamp,
    rows_in          bigint,
    rows_out         bigint,
    rows_quarantined bigint,
    duration_ms      bigint,
    PRIMARY KEY ((job_name, metric_date), batch_id)
) WITH CLUSTERING ORDER BY (batch_id DESC);
