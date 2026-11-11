#!/usr/bin/env bash
# sets up .env, venv, data and docker. safe to run again.

set -e

cd "$(dirname "$0")"

MONTHS=("2023-01")
DOWNLOAD_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DIR="data/raw"

# find python 3.10+
find_python() {
    for candidate in "py -3" python3 python; do
        if $candidate -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" > /dev/null 2>&1; then
            echo "$candidate"
            return
        fi
    done
}

echo "===== 1/5  .env ====="
if [ -f .env ]; then
    echo ".env already exists."
else
    cp .env.example .env
    echo "Created .env from .env.example"
fi

echo
echo "===== 2/5  Python virtual environment ====="
if [ -d .venv ]; then
    echo ".venv already exists."
else
    PYTHON="$(find_python)"
    if [ -z "$PYTHON" ]; then
        echo "Python 3.10 or newer is required. Install it and run setup.sh again."
        exit 1
    fi
    echo "Creating .venv with: $PYTHON ($($PYTHON --version))"
    $PYTHON -m venv .venv
fi

# Scripts/ on windows, bin/ on linux/mac
if [ -f .venv/Scripts/python.exe ]; then
    VENV_PYTHON=".venv/Scripts/python.exe"
else
    VENV_PYTHON=".venv/bin/python"
fi
echo "Installing requirements.txt..."
"$VENV_PYTHON" -m pip install --quiet --upgrade pip
"$VENV_PYTHON" -m pip install --quiet -r requirements.txt

echo
echo "===== 3/5  Taxi data (curl) ====="
mkdir -p "$RAW_DIR"
for month in "${MONTHS[@]}"; do
    file="$RAW_DIR/yellow_tripdata_${month}.parquet"
    if [ -f "$file" ]; then
        echo "Already downloaded: $file"
    else
        echo "Downloading $DOWNLOAD_URL/yellow_tripdata_${month}.parquet"
        # download to .part first so a broken download isn't used
        curl -fL -o "$file.part" "$DOWNLOAD_URL/yellow_tripdata_${month}.parquet"
        mv "$file.part" "$file"
    fi
done

echo
echo "===== 4/5  Docker services (Kafka, ClickHouse, producer, processor, monitoring) ====="
mkdir -p data/daily

# start containers only if something isn't running
running="$(docker compose ps --services --status running)"
missing=""
for service in kafka-1 kafka-2 kafka-3 clickhouse producer processor kafka-exporter prometheus grafana; do
    if ! echo "$running" | grep -qx "$service"; then
        missing="$missing $service"
    fi
done

if [ -z "$missing" ]; then
    echo "All services are already running."
    echo "(After changing app/ code, rebuild with: docker compose up -d --build producer processor)"
else
    echo "Not running:$missing"
    docker compose up -d --build
fi
docker compose ps

echo
echo "Setup finished. The pipeline is running."
echo "  Trips appear in ClickHouse taxi_stream.fact_taxi_trips, windows in taxi_stream.trip_window_agg"
echo
echo "  Logs:       docker compose logs -f producer processor"
echo "  Stop apps:  docker compose stop producer processor"
echo "  Stop all:   docker compose down        (data is kept)"
echo
echo "  Kafka (from the host): localhost:19092, localhost:29092, localhost:39092"
echo "  ClickHouse (HTTP):     localhost:8123"
echo "  Grafana dashboard:     http://localhost:3000   (admin / admin)"
echo "  Prometheus:            http://localhost:9090"

echo
echo "===== 5/5  Next 4 five-minute files -> data/daily ====="
"$VENV_PYTHON" scripts/copy_5min.py
echo "  Run again for the next 4 files:  $VENV_PYTHON scripts/copy_5min.py"
