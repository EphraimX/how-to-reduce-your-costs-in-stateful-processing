from threat_data_generator import threat_data
from threat_assessment import assess_threat


threat_data_response = threat_data()
threat_assessor = assess_threat()


# Initialize an empty list to store the states
states = []


try:
    print("Try")
    # Poll for data and process it
    for message in threat_assessor:
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