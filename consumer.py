from confluent_kafka import Consumer, KafkaException

KAFKA_BROKER = 'localhost:9092'  # Change if using a remote broker
TOPIC = 'test-topic'
GROUP_ID = 'test-group'

# Configure the consumer
consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'  # Start from the beginning if no offset is stored
}

consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])

try:
    print("🔄 Listening for messages...")
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for new messages
        if msg is None:
            continue
        if msg.error():
            print(f"⚠️ Consumer error: {msg.error()}")
            continue

        print(f"📥 Received: {msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped.")

finally:
    consumer.close()
