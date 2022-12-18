from kafka import KafkaProducer
from json import dumps
import random


# initializing the producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x:dumps(x).encode('utf-8'),
    api_version = (0, 10, 2)
)


# random values as products distance from final destination
for product_id in range(2000):

    distance_from_destination = random.randint(100, 1000)
    data = {
        'Distance From Destination in KM'  : distance_from_destination
    }
    
    # send data to kafka topic
    producer.send("rudders_technology", data)
    
# Ensure that all messages in the producer's buffer are sent to the Kafka cluster.
producer.flush()