from kafka import KafkaProducer
from json import dumps
import random


# Initialize the producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x:dumps(x).encode('utf-8'),
    api_version = (0, 10, 2)
)


# Random values as products' distance from final destination
for product_id in range(2000):

    distance_from_destination = random.randint(100, 1000)
    data = {
        'Distance From Destination in KM'  : distance_from_destination
    }
    
    # Send data to Kafka topic
    producer.send("rudders_technology", data)
    
# Ensure that all messages in the producer's buffer are sent to the Kafka cluster
producer.flush()
