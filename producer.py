from confluent_kafka import Producer
import time

KAFKA_BROKER = 'localhost:9092'  # Change if using a remote broker
TOPIC = 'test-topic'

# Configure the producer
producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    """Callback for delivery reports"""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

for i in range(10):
    message = f"Message {i}"
    producer.produce(TOPIC, message.encode('utf-8'), callback=delivery_report)
    print(f"📤 Sent: {message}")
    producer.flush()  # Ensure delivery before exiting
    time.sleep(1)

print("✅ All messages sent.")
