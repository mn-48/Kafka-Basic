# from confluent_kafka import Producer
# import time

# # KAFKA_BROKER = 'localhost:9092'  # Change if using a remote broker
# # KAFKA_BROKER = '127.0.0.1:9092'  # Change if using a remote broker
# KAFKA_BROKER = 'kafka::9092'  # Change if using a remote broker

# TOPIC = 'test-topic'

# # Configure the producer
# producer_conf = {'bootstrap.servers': KAFKA_BROKER}
# producer = Producer(producer_conf)

# def delivery_report(err, msg):
#     """Callback for delivery reports"""
#     if err is not None:
#         print(f"❌ Message delivery failed: {err}")
#     else:
#         print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# for i in range(10):
#     message = f"Message {i}"
#     producer.produce(TOPIC, message.encode('utf-8'), callback=delivery_report)
#     print(f"📤 Sent: {message}")
#     producer.flush()  # Ensure delivery before exiting
#     time.sleep(1)

# print("✅ All messages sent.")



from confluent_kafka import Producer
import time
import json

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_messages(bootstrap_servers, topic):
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'python-producer'
    }
    
    producer = Producer(conf)
    
    try:
        for i in range(1, 11):
            message = {
                'message_id': i,
                'text': f'This is message #{i}',
                'timestamp': int(time.time())
            }
            
            producer.produce(
                topic,
                key=str(i),
                value=json.dumps(message),
                callback=delivery_report
            )
            
            # Wait up to 1 second for events. Callbacks will be invoked during
            # this method call if the message is acknowledged.
            producer.poll(1)
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        pass
    finally:
        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        producer.flush()
        print("Message production completed")

if __name__ == '__main__':
    # Try localhost first, fall back to kafka hostname
    try:
        bootstrap_servers = 'localhost:9092'
        test_producer = Producer({'bootstrap.servers': bootstrap_servers})
        test_producer.list_topics(timeout=5)  # Test connection
    except Exception as e:
        print(f"Localhost connection failed, trying Docker internal network...")
        bootstrap_servers = 'kafka:9092'
    
    topic = 'test_topic'
    print(f"Producing messages to {bootstrap_servers} on topic {topic}")
    produce_messages(bootstrap_servers, topic)