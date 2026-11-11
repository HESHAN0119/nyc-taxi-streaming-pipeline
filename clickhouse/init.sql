-- Runs once, on the first start of ClickHouse (empty volume).
-- Times are stored as they are in the source; 'UTC' just stops any time zone conversion.

CREATE DATABASE IF NOT EXISTS taxi_stream;

-- cleaned trips, one row per trip_id (duplicates removed by ReplacingMergeTree, use FINAL)
CREATE TABLE IF NOT EXISTS taxi_stream.fact_taxi_trips
(
    trip_id                String,                       -- file name + row number
    vendor_id              UInt8,
    pickup_datetime        DateTime('UTC'),
    dropoff_datetime       DateTime('UTC'),
    passenger_count        Nullable(UInt8),              -- 0 -> NULL
    trip_distance          Float64,
    rate_code_id           UInt8,                        -- missing -> 99
    store_and_fwd_flag     Nullable(String),
    pickup_location_id     UInt16,
    dropoff_location_id    UInt16,
    payment_type_id        UInt8,
    fare_amount            Decimal(10, 2),
    extra                  Nullable(Decimal(10, 2)),
    mta_tax                Nullable(Decimal(10, 2)),
    tip_amount             Nullable(Decimal(10, 2)),
    tolls_amount           Nullable(Decimal(10, 2)),
    improvement_surcharge  Nullable(Decimal(10, 2)),
    total_amount           Decimal(10, 2),
    congestion_surcharge   Nullable(Decimal(10, 2)),
    airport_fee            Nullable(Decimal(10, 2)),
    trip_duration_min      Float32,
    source_file            LowCardinality(String),
    load_id                LowCardinality(String),       -- time the file was sent
    kafka_partition        UInt8,
    kafka_offset           UInt64,
    produced_at            DateTime64(3, 'UTC'),
    ingested_at            DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY trip_id;

-- trips that failed cleaning, stored as they came from Kafka
CREATE TABLE IF NOT EXISTS taxi_stream.rejected_trips
(
    raw_event        String,                       -- the JSON from Kafka
    reason           LowCardinality(String),
    source_file      LowCardinality(String),
    kafka_partition  UInt8,
    kafka_offset     UInt64,
    rejected_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(rejected_at)
ORDER BY (kafka_partition, kafka_offset);

-- sliding window totals; saving a window again with a newer version = upsert
CREATE TABLE IF NOT EXISTS taxi_stream.trip_window_agg
(
    window_start    DateTime('UTC'),
    window_end      DateTime('UTC'),
    pu_location_id  UInt16,
    kafka_partition Int16 DEFAULT -1,
    trip_count      UInt32,
    total_fare      Decimal(12, 2),
    total_tips      Decimal(12, 2),
    total_revenue   Decimal(12, 2),
    total_distance  Float64,
    is_final        Bool,                          -- window closed, won't change
    version         DateTime64(3, 'UTC')           -- newest version wins
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, pu_location_id);

-- latest version of each window, with averages
CREATE VIEW IF NOT EXISTS taxi_stream.v_window_latest AS
SELECT
    window_start,
    window_end,
    pu_location_id,
    trip_count,
    total_fare,
    total_tips,
    total_revenue,
    round(total_distance, 2)                                        AS total_distance,
    round(total_fare / trip_count, 2)                               AS avg_fare,
    if(total_distance > 0, round(total_fare / total_distance, 2), 0) AS avg_fare_per_mile,
    is_final,
    version                                                         AS updated_at
FROM taxi_stream.trip_window_agg FINAL;
