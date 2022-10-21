import json
import time
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_detail"
ORDER_LIMIT = 15

producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Going to be generating order after 5 seconds")
print("WIll generate one unique order every 5 seconds")

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "cost": i*2,
        "items": "Burger, Sandwich"
    }

    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode("utf-8")
    )
    print(f"Done sending....{i}")
    time.sleep(5)