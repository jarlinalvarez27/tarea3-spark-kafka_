import json, time, random, os
from kafka import KafkaProducer

TOPIC = os.getenv("TOPIC", "eventos_sensores")
BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

sensor_ids = [f"S{i:03d}" for i in range(1, 21)]

def evento():
    return {
        "sensor_id": random.choice(sensor_ids),
        "ts": int(time.time()*1000),
        "temp": round(random.uniform(18, 40), 2),
        "hum": round(random.uniform(35, 95), 2),
        "ok": random.random() > 0.05
    }

if __name__ == "__main__":
    print(f"Enviando eventos a {TOPIC} @ {BOOTSTRAP} ... Ctrl+C para salir")
    while True:
        producer.send(TOPIC, evento())
        producer.flush()
        time.sleep(0.2)
