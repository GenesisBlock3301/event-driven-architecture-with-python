import json
from kafka import KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092"
)

total_order_count = 0
total_revenue = 0
print("Analytic listing..")

while True:
    for message in consumer:
        print("Updating analytic ..")
        consumed_message = json.loads(message.value.decode())
        total_cost = float(consumed_message["total_cost"])
        total_order_count += 1
        total_revenue += total_cost

        print(f"Order so far today: {total_order_count}")
        print(f"Revenue so far today: {total_revenue}")
