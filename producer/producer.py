import json
import time
import csv
import os
from kafka import KafkaProducer

# Path to the data file (mounted inside container)
data_file = "/app/data/taxi_data.csv"

if not os.path.exists(data_file):
    raise FileNotFoundError(f"Data file '{data_file}' not found!")

# Try connecting to Kafka with retries
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5
        )
        print("✅ Connected to Kafka!")
    except Exception as e:
        print("❌ Kafka not ready, retrying in 3s...")
        time.sleep(3)

# Read the CSV and send messages
with open(data_file, "r", newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Clean up numeric fields
        if "trip_distance" in row:
            row["trip_distance"] = float(row["trip_distance"])
        if "fare_amount" in row:
            row["fare_amount"] = float(row["fare_amount"])
        if "passenger_count" in row:
            row["passenger_count"] = int(row["passenger_count"])

        # Send message to Kafka
        producer.send("taxi_trips", row)
        print(f"📤 Sent trip {row.get('trip_id', '')} to taxi_trips")

        time.sleep(2)  # simulate 1 record every 2s

# Flush any remaining messages
producer.flush()
print("✅ All messages sent and flushed.")
