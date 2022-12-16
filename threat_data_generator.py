from kafka import KafkaProducer
import random


# Create a KafkaProducer instance
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x:dumps(x).encode('utf-8'),
    api_version=(20, 2, 1)
)


def threat_data():

    # Define the topic to which the data will be published
    topic = "vehicle_threat_assessment_level"

    # Publish data to the topic using the producer.send() method
    for i in range(1, 200):

        data = {
            "vehicle_id" : i,
            "threat_level" : random.random()
        }

        if data["threat_level"] > 0.75:
            data["is_threat"] = 1

        producer.send(topic, data)


    producer.flush()

    return "Data Successfully Sent"