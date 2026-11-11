"""Saves trips that failed cleaning into rejected_trips, as they came from Kafka."""

COLUMNS = ["raw_event", "reason", "source_file", "kafka_partition", "kafka_offset"]


def save(clickhouse, rejected):
    # rejected = list of [raw_event, reason, source_file, kafka_partition, kafka_offset]
    if rejected:
        clickhouse.insert("rejected_trips", rejected, column_names=COLUMNS)
