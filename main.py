from kafka import KafkaProducer
import json

# Create a KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

# Define the topic to which the data will be published
topic = "vehicle_real_time_monitoring"

# check if topic is in bytes and decode to string
if type(topic) == bytes:
    topic = topic.decode('utf-8')

# Publish data to the topic using the producer.send() method
for i in range(1, 100):
    data = {"vehicle_id": i, "speed": i * 10, "fuel_level": i * 5, "engine_temp": i * 20}
    data = json.dumps(data).encode('utf-8')
    producer.send(topic, data)

# Flush the producer to ensure all data is sent to Kafka
producer.flush()


from kafka import KafkaConsumer

# Create a KafkaConsumer instance
consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"], group_id="real-time-monitoring")

# Subscribe to the topic
consumer.subscribe([topic])

# Set the consumer to poll for data every 10 or 20 seconds
consumer.poll(10)

# Process the data from the topic
for message in consumer:
    print(f"Received message: {message.value}")

# Tell Kafka that the message has been processed
consumer.commit()


# Initialize an empty list to store the states
states = []


try:
    # Poll for data and process it
    for message in consumer:
        # Add the state data to the list
        states.append(message.value)

        # If the list reaches a certain size (e.g. 10), persist the data to the disk
        if len(states) == 10:
            with open("state_data.txt", "w") as f:
                f.write("\n".join(states))

            # Clear the list
            states = []

except:
    # In case of a disconnection, seek to the last processed offset
    consumer.seek_to_last_committed()
