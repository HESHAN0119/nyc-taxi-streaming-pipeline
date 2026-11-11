"""Producer: watches data/daily and sends every new parquet file to Kafka."""

import json
import time
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer, JsonSerializer
from prometheus_client import Counter, Gauge, start_http_server
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from common import PROJECT_ROOT, env, setup_logging

log = setup_logging("producer")

TOPIC = env("TOPIC_NAME")
WATCH_DIR = PROJECT_ROOT / env("WATCH_DIR", "data/daily")
SENT_FILES = PROJECT_ROOT / env("SENT_FILES", "data/sent_files.json")
RATE = float(env("PRODUCER_RATE", "20"))  # rows per second

# metrics
FILE_ROWS = Gauge("producer_file_rows", "Rows in the file", ["file"])
EVENTS_SENT = Counter("producer_events_sent_total", "Rows sent to Kafka", ["file"])

producer = KafkaProducer(
    bootstrap_servers=env("KAFKA_BOOTSTRAP_SERVERS").split(","),
    client_id="taxi-producer",
    acks="all",               # wait for all in-sync replicas
    enable_idempotence=True,  # no duplicates on retry
    linger_ms=50,
    compression_type="gzip",
    key_serializer=DefaultSerializer(),
    value_serializer=JsonSerializer(),
)


def read_sent_files():
    if SENT_FILES.exists():
        return json.loads(SENT_FILES.read_text())
    return {}


def save_sent_file(name, rows):
    sent = read_sent_files()
    sent[name] = {"rows": rows, "sent_at": datetime.now().isoformat(timespec="seconds")}
    SENT_FILES.write_text(json.dumps(sent, indent=2))


def wait_until_copied(path):
    # wait until the file size stops changing
    size = -1
    while path.stat().st_size != size:
        size = path.stat().st_size
        time.sleep(2)


def send_file(path):
    table = pq.read_table(path)
    table = table.append_column("row_number", pa.array(range(table.num_rows)))
    rows = table.sort_by("tpep_dropoff_datetime").to_pylist()
    FILE_ROWS.labels(path.name).set(len(rows))

    load_id = datetime.now().strftime("%Y%m%d%H%M%S")
    started = time.time()
    for count, row in enumerate(rows):
        event = {key: value.isoformat() if isinstance(value, datetime) else value
                 for key, value in row.items()}
        event["trip_id"] = f"{path.stem}-{event.pop('row_number')}"
        event["source_file"] = path.name
        event["load_id"] = load_id
        event["produced_at"] = int(time.time() * 1000)

        time.sleep(max(0, started + count / RATE - time.time()))  # rate limit
        producer.send(TOPIC, key=str(event["PULocationID"]), value=event)  # same zone -> same partition
        EVENTS_SENT.labels(path.name).inc()

    producer.flush()
    return len(rows)


def on_new_file(path):
    try:
        # skip non-parquet, deleted or already sent files
        if path.suffix != ".parquet" or not path.exists() or path.name in read_sent_files():
            return
        wait_until_copied(path)
        log.info("Sending file", extra={"file": path.name, "rate": RATE})
        rows = send_file(path)
        save_sent_file(path.name, rows)
        log.info("File sent", extra={"file": path.name, "rows": rows})
    except Exception:
        log.exception("Could not send file", extra={"file": path.name})


def main():
    start_http_server(int(env("PRODUCER_METRICS_PORT", "8000")))

    # files copied while the producer was down
    for path in sorted(WATCH_DIR.glob("*.parquet")):
        on_new_file(path)

    handler = FileSystemEventHandler()
    handler.on_any_event = lambda event: on_new_file(Path(event.dest_path or event.src_path))
    # docker on windows doesn't get file events, so poll every 2s there
    observer = PollingObserver(timeout=2) if env("WATCH_POLLING", "false") == "true" else Observer()
    observer.schedule(handler, str(WATCH_DIR))
    observer.start()
    log.info("Watching folder", extra={"folder": str(WATCH_DIR), "rate": RATE})

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        log.info("Producer stopped")


if __name__ == "__main__":
    main()
