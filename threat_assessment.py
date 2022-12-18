from kafka import KafkaConsumer
from time import time


def count_threats(messages):
    threat_count = 0
    for message in messages:
        if message["is_threat"]:
            threat_count += 1
    return threat_count


def assess_threat():

    # Create a KafkaConsumer instance
    consumer = KafkaConsumer(
        "topic_test", 
        group_id='esq-threat-assessment', 
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 9)
    )

    # Set the consumer to poll for data every 10 or 20 seconds
    consumer.poll(10)

    # Process the data from the topic
    for message in consumer:
        print(f"Received message: {message.value}")

    # Tell Kafka that the message has been processed
    consumer.commit()
        

assess_threat()