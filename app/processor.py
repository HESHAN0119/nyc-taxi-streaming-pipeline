"""Processor: reads trips from Kafka, cleans them, inserts them into ClickHouse and updates the windows.

Windows are saved and offsets committed every WINDOW_TRIGGER_MINUTES. If it crashes, Docker restarts it
and Kafka sends everything after the last commit again.
"""

import json
import re
import time
from datetime import datetime, timedelta, timezone

import clickhouse_connect
from kafka import KafkaConsumer
from prometheus_client import Counter, start_http_server

import rejected
import sliding_window
from common import env, setup_logging

log = setup_logging("processor")

BATCH_SIZE = int(env("PROCESSOR_BATCH_SIZE", "500"))
BATCH_SECONDS = float(env("PROCESSOR_FLUSH_SECONDS", "5"))
TRIGGER_SECONDS = float(env("WINDOW_TRIGGER_MINUTES", "5")) * 60

# parquet column -> table column
RENAME = {
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "ratecodeid": "rate_code_id",
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
    "payment_type": "payment_type_id",
}

FACT_COLUMNS = [
    "trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count", "trip_distance",
    "rate_code_id", "store_and_fwd_flag", "pickup_location_id", "dropoff_location_id", "payment_type_id",
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge", "airport_fee", "trip_duration_min", "source_file", "load_id",
    "kafka_partition", "kafka_offset", "produced_at",
]

REQUIRED = ["vendor_id", "pickup_datetime", "dropoff_datetime", "pickup_location_id",
            "dropoff_location_id", "payment_type_id", "trip_distance", "fare_amount", "total_amount"]

# metrics
EVENTS_CONSUMED = Counter("processor_events_consumed_total", "Trips read from Kafka", ["file"])
EVENTS_REJECTED = Counter("processor_events_rejected_total", "Trips not inserted (failed cleaning)", ["file", "reason"])
ROWS_INSERTED = Counter("processor_rows_inserted_total", "Trips inserted into fact_taxi_trips", ["file"])


def read_batch(consumer):
    # read until BATCH_SIZE trips or BATCH_SECONDS
    messages = []
    started = time.time()
    while len(messages) < BATCH_SIZE and time.time() - started < BATCH_SECONDS:
        for records in consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE - len(messages)).values():
            messages.extend(records)
    return messages


def clean(event):
    # returns (trip, None) if ok, (trip, reason) if bad
    trip = {RENAME.get(key.lower(), key.lower()): value for key, value in event.items()}

    if any(trip.get(column) is None for column in REQUIRED):
        return trip, "missing_required_field"

    trip["pickup_datetime"] = datetime.fromisoformat(trip["pickup_datetime"])
    trip["dropoff_datetime"] = datetime.fromisoformat(trip["dropoff_datetime"])
    duration = trip["dropoff_datetime"] - trip["pickup_datetime"]
    trip["trip_duration_min"] = round(duration.total_seconds() / 60, 2)

    # pickup must be on the date in the file name
    file_date = re.search(r"\d{4}-\d{2}(-\d{2})?", trip.get("source_file") or "")
    if file_date and not trip["pickup_datetime"].isoformat().startswith(file_date.group(0)):
        return trip, "pickup_outside_file_date"
    if duration <= timedelta(0):
        return trip, "dropoff_not_after_pickup"
    if duration > timedelta(hours=24):
        return trip, "duration_over_24h"
    if trip["fare_amount"] < 0 or trip["total_amount"] < 0:
        return trip, "negative_amount"

    trip["rate_code_id"] = int(trip["rate_code_id"]) if trip.get("rate_code_id") is not None else 99  # 99 = unknown
    trip["passenger_count"] = int(trip["passenger_count"]) if trip.get("passenger_count") else None
    for column in ["vendor_id", "pickup_location_id", "dropoff_location_id", "payment_type_id"]:
        trip[column] = int(trip[column])
    return trip, None


def insert_facts(clickhouse, trips):
    rows = []
    for trip in trips:
        row = dict(trip)
        row["pickup_datetime"] = sliding_window.utc(trip["pickup_datetime"])
        row["dropoff_datetime"] = sliding_window.utc(trip["dropoff_datetime"])
        row["produced_at"] = datetime.fromtimestamp((trip.get("produced_at") or 0) / 1000, timezone.utc)
        rows.append([row.get(column) for column in FACT_COLUMNS])
    clickhouse.insert("fact_taxi_trips", rows, column_names=FACT_COLUMNS)
    for trip in trips:
        ROWS_INSERTED.labels(trip["source_file"]).inc()


def save_and_commit(clickhouse, consumer):
    # save windows first, then commit
    saved = sliding_window.save(clickhouse)
    consumer.commit()
    log.info("Windows saved", extra={"windows": saved, "open_windows": len(sliding_window.windows)})


def main():
    start_http_server(int(env("PROCESSOR_METRICS_PORT", "8001")))

    clickhouse = clickhouse_connect.get_client(
        host=env("CLICKHOUSE_HOST", "localhost"), port=int(env("CLICKHOUSE_HTTP_PORT", "8123")),
        username=env("CLICKHOUSE_USER"), password=env("CLICKHOUSE_PASSWORD"), database=env("CLICKHOUSE_DB"),
    )
    consumer = KafkaConsumer(
        env("TOPIC_NAME"),
        bootstrap_servers=env("KAFKA_BOOTSTRAP_SERVERS").split(","),
        group_id=env("CONSUMER_GROUP", "taxi-processor"),
        client_id="taxi-processor",
        enable_auto_commit=False,      # we commit ourselves
        auto_offset_reset="earliest",
    )
    sliding_window.load_state(clickhouse)  # continue from last run
    log.info("Processor started", extra={"batch_size": BATCH_SIZE, "batch_seconds": BATCH_SECONDS,
                                         "trigger_seconds": TRIGGER_SECONDS,
                                         "open_windows": len(sliding_window.windows)})
    last_trigger = time.time()

    try:
        while True:
            messages = read_batch(consumer)
            if time.time() - last_trigger >= TRIGGER_SECONDS:
                save_and_commit(clickhouse, consumer)
                last_trigger = time.time()
            if not messages:
                continue

            trips = []
            bad = []
            for message in messages:
                file = "unknown"
                try:
                    event = json.loads(message.value)
                    file = event.get("source_file") or "unknown"
                    trip, reason = clean(event)
                except Exception:
                    reason = "malformed_event"
                EVENTS_CONSUMED.labels(file).inc()
                if reason:
                    EVENTS_REJECTED.labels(file, reason).inc()
                    bad.append([message.value.decode("utf-8", "replace"), reason, file,
                                message.partition, message.offset])
                    continue
                trip["kafka_partition"] = message.partition
                trip["kafka_offset"] = message.offset
                trips.append(trip)

            late = 0
            if trips:
                insert_facts(clickhouse, trips)
                late = sliding_window.update(trips)
            rejected.save(clickhouse, bad)

            log.info("Batch done", extra={"read": len(messages), "inserted": len(trips),
                                          "rejected": len(messages) - len(trips), "late": late})
    except KeyboardInterrupt:
        save_and_commit(clickhouse, consumer)
        log.info("Processor stopped")
    except Exception:
        log.exception("Processor failed")
        raise
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
