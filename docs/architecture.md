# Architecture: how the data flows

```mermaid
flowchart LR
    subgraph HOST["Windows host"]
        RAW["data/raw<br/>monthly parquet (curl in setup.sh)<br/><i>not watched</i>"]
        COPY["<b>scripts/copy_5min.py</b><br/>splits 2023-01-01 into 5 min files<br/>copies the next 4 per run"]
        DAILY["data/daily<br/>one parquet file per 5 minutes<br/><i>watched by the producer</i>"]
        SENT["data/sent_files.json<br/>names of files already sent"]
    end

    subgraph DOCKER["Docker Compose"]
        PRODUCER["<b>producer.py</b><br/>1. watchdog sees a new file<br/>2. skip if name in sent_files.json<br/>3. sort rows by dropoff time<br/>4. send 10-50 rows/s<br/>key = pickup zone"]

        subgraph KAFKA["Kafka cluster (KRaft, 3 brokers)"]
            TOPIC["topic <b>taxi-trips-stream</b><br/>6 partitions<br/>replication factor 3<br/>min.insync.replicas 2"]
        end

        PROCESSOR["<b>processor.py</b><br/>1. read batch (500 rows or 5 s)<br/>2. clean (Assignment 1 rules)<br/>3. insert into fact table<br/>4. sliding_window.update()<br/>5. every 5 min: save windows + commit"]

        WINDOW["<b>sliding_window.py</b><br/>10-min windows every 5 min<br/>saved every 5 min (trigger)<br/>per pickup zone<br/>watermark = newest - 2 min<br/>late trips not counted"]

        subgraph CH["ClickHouse (database taxi_stream)"]
            FACT["<b>fact_taxi_trips</b><br/>one row per clean trip<br/>dedup by trip_id"]
            REJ["<b>rejected_trips</b><br/>bad trips: raw JSON + reason"]
            AGG["<b>trip_window_agg</b><br/>window totals per zone<br/>upsert by version"]
            VIEW["<b>v_window_latest</b><br/>latest window + averages"]
        end

        subgraph MON["Monitoring"]
            PROM["Prometheus"]
            GRAF["Grafana"]
        end
    end

    RAW --> COPY
    COPY -- "yellow_tripdata_2023-01-01_0005.parquet" --> DAILY
    DAILY --> PRODUCER
    PRODUCER <--> SENT
    PRODUCER -- "JSON rows<br/>acks=all, idempotent" --> TOPIC
    TOPIC -- "consumer group<br/>taxi-processor" --> PROCESSOR
    PROCESSOR -- "batch insert" --> FACT
    PROCESSOR -- "bad trips" --> REJ
    PROCESSOR -- "same batch" --> WINDOW
    WINDOW -- "upsert windows" --> AGG
    AGG --> VIEW
    WINDOW -. "load open windows<br/>at startup" .-> AGG

    PRODUCER -. ":8000/metrics" .-> PROM
    PROCESSOR -. ":8001/metrics" .-> PROM
    TOPIC -. "kafka-exporter<br/>consumer lag" .-> PROM
    PROM --> GRAF
```

## One trip, step by step

1. `python scripts/copy_5min.py` copies the next 4 five-minute files of 2023-01-01 (split by dropoff time)
   into `data/daily`, e.g. `yellow_tripdata_2023-01-01_0005.parquet`.
2. **producer.py** (watchdog) sees it, checks `sent_files.json`, and sends every row to Kafka as JSON,
   sorted by dropoff time, 10-50 rows per second. Key = pickup zone, so a zone always goes to the same partition.
3. **Kafka** stores each row in one of 6 partitions, copied to 3 brokers.
4. **processor.py** reads a batch, cleans it, inserts the good trips into **fact_taxi_trips**,
   and passes the same batch to **sliding_window.py**.
5. **sliding_window.py** adds each trip to its 2 windows in memory; every 5 minutes (trigger) the windows are upserted into
   **trip_window_agg** and the Kafka offsets are committed.
   Trips older than the watermark are late: they are in the fact table but not in the windows.
6. **Prometheus** collects the metrics of the producer, the processor and Kafka; **Grafana** shows them.
