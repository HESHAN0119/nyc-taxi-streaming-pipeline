"""Splits 2023-01-01 into 5 minute parquet files and copies the next 4 to data/daily.

    .venv/Scripts/python.exe scripts/copy_5min.py

Each run copies 4 files, 10 seconds apart (00:00 ... 00:15, next run 00:20 ... 00:35).
"""

import json
import shutil
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.parquet as pq

PROJECT = Path(__file__).resolve().parents[1]
URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
MONTHLY_FILE = PROJECT / "data" / "raw" / "yellow_tripdata_2023-01.parquet"
OUT_DIR = PROJECT / "data" / "5min" / "2023-01-01"
DAILY_DIR = PROJECT / "data" / "daily"
SENT_FILES = PROJECT / "data" / "sent_files.json"

DAY = datetime(2023, 1, 1)
NEXT_DAY = DAY + timedelta(days=1)

FILES_PER_RUN = 4
DELAY_SECONDS = 10   # wait between files


def download():
    if MONTHLY_FILE.exists():
        return
    print("Downloading", URL)
    MONTHLY_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = MONTHLY_FILE.with_suffix(".parquet.part")
    urllib.request.urlretrieve(URL, tmp)
    tmp.replace(MONTHLY_FILE)


def make_5min_files():
    if OUT_DIR.exists() and any(OUT_DIR.glob("*.parquet")):
        return
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # split by dropoff time (= event time), so the files are in order for the windows
    day_data = pq.read_table(MONTHLY_FILE, filters=[("tpep_dropoff_datetime", ">=", DAY),
                                                    ("tpep_dropoff_datetime", "<", NEXT_DAY)])
    print("Trips on", DAY.date(), ":", day_data.num_rows)

    start = DAY
    while start < NEXT_DAY:
        end = start + timedelta(minutes=5)
        dropoff = day_data["tpep_dropoff_datetime"]
        part = day_data.filter(pc.and_(pc.greater_equal(dropoff, start), pc.less(dropoff, end)))
        if part.num_rows > 0:
            pq.write_table(part, OUT_DIR / f"yellow_tripdata_{DAY:%Y-%m-%d}_{start:%H%M}.parquet")
        start = end
    print("Created 5 minute files in", OUT_DIR)


def copy_next_file():
    sent = json.loads(SENT_FILES.read_text()) if SENT_FILES.exists() else {}
    for file in sorted(OUT_DIR.glob("*.parquet")):
        if (DAILY_DIR / file.name).exists() or file.name in sent:
            continue
        # copy to .part first, then rename, so watchdog never sees a half-copied file
        DAILY_DIR.mkdir(parents=True, exist_ok=True)
        tmp = DAILY_DIR / (file.name + ".part")
        shutil.copy(file, tmp)
        tmp.replace(DAILY_DIR / file.name)
        print("Copied", file.name, "-> data/daily")
        return True
    print("All 5 minute files are already copied.")
    return False


if __name__ == "__main__":
    download()
    make_5min_files()
    for i in range(FILES_PER_RUN):
        if not copy_next_file():
            break
        if i < FILES_PER_RUN - 1:
            time.sleep(DELAY_SECONDS)
