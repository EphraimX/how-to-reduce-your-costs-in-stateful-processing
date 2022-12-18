from kafka import KafkaConsumer
from json import loads
import time
import json

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    "rudders_technology",
    bootstrap_servers=["localhost:9092"],
    value_deserializer=lambda value: json.loads(value.decode('utf-8')),
    group_id="rudders-technology-stream",
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    consumer_timeout_ms=1000,
    api_version = (0,9)
)


states = []
previous_time = time.time()


def write_to_disk(items, current_time):
    # Open the file in write mode
    with open("Products Distance To Final Destination.txt", "w") as f:
        write_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))
        f.write(str(write_time)+"\n")
        # Write each dictionary to the file as a separate JSON object
        for item in items:
            # Loop through the list of dictionaries
            json.dump(item, f)
            # Add a newline character after each dictionary
            f.write("\n")
        print("Writing to disk successful")



# Process the data from the topic
for message in consumer:

    current_time = time.time()

    # Accumulating Data
    states.append(message)
    
    print(message.value)

    # Process Data Every 20 Seconds
    if current_time - previous_time >= 20:
        # Persisting/Saving the data to disk
        print("Writing to Disk")
        write_to_disk(states, current_time)
        # update the previous time to current time for the next iteration
        previous_time = current_time
        # Empty the states list for accumulation of new data
        states = []
        # Sleep the machine for two seconds before running through the loop
        time.sleep(2)
    

# Tell Kafka that the message has been processed
consumer.commit()
print("Writing to disk completed")