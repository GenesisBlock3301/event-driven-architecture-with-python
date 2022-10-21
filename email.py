import json

from kafka import KafkaProducer, KafkaConsumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

consumer = KafkaConsumer(
    ORDER_CONFIRMED_KAFKA_TOPIC,
    bootstrap_servers="localhost:29092"
)

email_send_so_far = set()
print("Going to start listening..")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message["customer_email"]
        email_send_so_far.add(customer_email)
        print(f"So far emails sent to {len(email_send_so_far)} unique email..")
