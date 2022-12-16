from kafka import KafkaConsumer


def assess_threat():

    # Create a KafkaConsumer instance
    consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"], group_id="real-time-monitoring")

    # Subscribe to the topic
    consumer.subscribe([topic])

    # Set the consumer to poll for data every 20 seconds
    consumer.poll(20)

    # Process the data from the topic
    for message in consumer:
        print(f"Received message: {message.value}")

    # Tell Kafka that the message has been processed
    consumer.commit()


    return consumer