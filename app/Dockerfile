# image for producer.py and processor.py
FROM python:3.11-slim

WORKDIR /opt/taxi

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

# show logs right away
ENV PYTHONUNBUFFERED=1
