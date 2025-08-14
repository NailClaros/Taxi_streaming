# producer.py
import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5
)

def random_trip():
    return {
        "trip_id": f"trip_{random.randint(1,1_000_000)}",
        "pickup_datetime": datetime.now(timezone.utc).isoformat(),
        "pickup_lat": 40.7 + (random.random() - 0.5) * 0.2,
        "pickup_long": -74.0 + (random.random() - 0.5) * 0.2,
        "dropoff_lat": 40.7 + (random.random() - 0.5) * 0.2,
        "dropoff_long": -74.0 + (random.random() - 0.5) * 0.2,
        "fare_amount": round(random.uniform(3.0, 60.0), 2),
        "passenger_count": random.randint(1,4),
        "vendor_id": random.choice(["V1","V2"]),
        "pickup_borough": random.choice(["Manhattan","Brooklyn","Queens","Bronx","Staten Island"])
    }

if __name__ == "__main__":
    while True:
        ev = random_trip()
        producer.send("taxi_trips", ev)
        producer.flush()
        print("sent:", ev)
        time.sleep(5)
