# Kafka-Basic


### [download](https://dlcdn.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz)
```
$ tar -xzf kafka_2.13-3.9.0.tgz
$ cd kafka_2.13-3.9.0
```

# Start the ZooKeeper service (always Run it/ don't close terminal)
`bin/zookeeper-server-start.sh config/zookeeper.properties`


# Start the Kafka broker service (Run it in different terminal)
`bin/kafka-server-start.sh config/server.properties`


# Create Topic
`bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092`


# Kafta topics view
`bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092`


# Write some events into the topic

`bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092`

>This is my first event
>This is my second event


# Read the events
`bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092`
