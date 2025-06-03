# from confluent_kafka import Consumer, KafkaException

# # KAFKA_BROKER = 'localhost:9092'  # Change if using a remote broker
# # KAFKA_BROKER = '127.0.0.1:9092'  # Change if using a remote broker
# KAFKA_BROKER = 'kafka::9092'  # Change if using a remote broker
# TOPIC = 'test-topic'
# GROUP_ID = 'test-group'

# # Configure the consumer
# consumer_conf = {
#     'bootstrap.servers': KAFKA_BROKER,
#     'group.id': GROUP_ID,
#     'auto.offset.reset': 'earliest'  # Start from the beginning if no offset is stored
# }

# consumer = Consumer(consumer_conf)
# consumer.subscribe([TOPIC])

# try:
#     print("🔄 Listening for messages...")
#     while True:
#         msg = consumer.poll(timeout=1.0)  # Poll for new messages
#         if msg is None:
#             continue
#         if msg.error():
#             print(f"⚠️ Consumer error: {msg.error()}")
#             continue

#         print(f"📥 Received: {msg.value().decode('utf-8')}")

# except KeyboardInterrupt:
#     print("\n🛑 Consumer stopped.")

# finally:
#     consumer.close()



from confluent_kafka import Consumer, KafkaException
import json
import time

def consume_messages(bootstrap_servers, topic):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'python-consumer-group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event
                    print(f"Reached end of {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                else:
                    print(f"Error: {msg.error()}")
            else:
                message = json.loads(msg.value())
                print(f"Consumed message: {message}")
                
                # Manually commit offset
                consumer.commit(asynchronous=False)
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Consumer closed")

if __name__ == '__main__':
    # For local access from host machine, use 'localhost:9092'
    # For access from another container in the same network, use 'kafka:9092'
    bootstrap_servers = 'localhost:9092'
    topic = 'test_topic'
    
    print(f"Consuming messages from {bootstrap_servers} on topic {topic}")
    consume_messages(bootstrap_servers, topic)