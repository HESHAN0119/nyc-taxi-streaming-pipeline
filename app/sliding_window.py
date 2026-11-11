"""Sliding windows: trips and revenue per pickup zone, 10 min windows every 5 min (by dropoff time).

Watermark = newest dropoff - 2 min, per partition. Older trips are late and not counted.
Windows that end before the watermark are final.
"""

from datetime import datetime, timedelta, timezone

from prometheus_client import Counter

from common import env

WINDOW = timedelta(minutes=int(env("WINDOW_MINUTES", "10")))
SLIDE = timedelta(minutes=int(env("WINDOW_SLIDE_MINUTES", "5")))
LATENESS = timedelta(minutes=int(env("ALLOWED_LATENESS_MINUTES", "2")))

TABLE = "trip_window_agg"
COLUMNS = ["window_start", "window_end", "pu_location_id", "kafka_partition", "trip_count",
           "total_fare", "total_tips", "total_revenue", "total_distance", "is_final", "version"]

LATE_TRIPS = Counter("processor_late_events_total", "Trips older than the watermark (not counted in windows)")

windows = {}    # (partition, window_start, zone) -> [count, fare, tips, revenue, distance]
watermark = {}  # partition -> time


def utc(value):
    return value.replace(tzinfo=timezone.utc)


def window_starts(time):
    # 10:07 -> [10:05, 10:00]
    slot = time.replace(minute=time.minute - time.minute % (SLIDE.seconds // 60), second=0, microsecond=0)
    return [slot - i * SLIDE for i in range(WINDOW // SLIDE)]


def load_state(clickhouse):
    # load watermarks and open windows from the last run
    for partition, end in clickhouse.query(
            f"SELECT kafka_partition, max(window_end) FROM {TABLE} FINAL "
            f"WHERE is_final AND kafka_partition >= 0 GROUP BY kafka_partition").result_rows:
        watermark[partition] = end.replace(tzinfo=None)

    for partition, start, zone, *totals in clickhouse.query(
            f"SELECT kafka_partition, window_start, pu_location_id, trip_count, total_fare, total_tips, "
            f"total_revenue, total_distance FROM {TABLE} FINAL WHERE NOT is_final AND kafka_partition >= 0").result_rows:
        windows[(partition, start.replace(tzinfo=None), zone)] = [float(value) for value in totals]


def update(trips):
    # add trips to windows in memory, return late count
    late = 0
    for trip in trips:
        partition = trip["kafka_partition"]
        time = trip["dropoff_datetime"]
        if partition in watermark and time < watermark[partition]:
            late += 1
            continue

        values = [1, trip["fare_amount"], trip.get("tip_amount") or 0, trip["total_amount"], trip["trip_distance"]]
        for start in window_starts(time):
            key = (partition, start, trip["pickup_location_id"])
            windows[key] = [old + new for old, new in zip(windows.get(key, [0] * 5), values)]

        watermark[partition] = max(watermark.get(partition, time - LATENESS), time - LATENESS)

    LATE_TRIPS.inc(late)
    return late


def save(clickhouse):
    # write all windows; final ones are removed from memory
    version = datetime.now(timezone.utc)
    rows = []
    for key, totals in list(windows.items()):
        partition, start, zone = key
        is_final = partition in watermark and start + WINDOW <= watermark[partition]
        rows.append([utc(start), utc(start + WINDOW), zone, partition, int(totals[0]),
                     round(totals[1], 2), round(totals[2], 2), round(totals[3], 2), round(totals[4], 3),
                     is_final, version])
        if is_final:
            del windows[key]
    if rows:
        clickhouse.insert(TABLE, rows, column_names=COLUMNS)
    return len(rows)
