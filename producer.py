import csv
import time
from confluent_kafka import Producer
import random
import json

kafka_conf = {
    'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka broker
    'client.id': 'test-topic'
}

# Tạo một producer Kafka
producer = Producer(kafka_conf)

# Function to send data to Kafka topic
def send_to_kafka(producer, data):
    topic_name = 'test-topic'
    key = 'card_transactions_info'

    # Convert dict to JSON string
    json_data = json.dumps(data)

    # Encode JSON string to bytes
    value = json_data.encode('utf-8')

    producer.produce(topic_name, key=key, value=value)
    producer.flush()


# CSV file path
csv_file_path = "D:\hoc\HK4_1\XLPTDLTT\data\credit_card_transactions-ibm_v2.csv"

# Read CSV file and send data to Kafka
with open(csv_file_path, 'r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        # Convert row to JSON or any desired format
        json_data = row  # Modify this part based on your data structure
        
        # Send data to Kafka topic
        send_to_kafka(producer, json_data)
        
        # Sleep for a random time between 1s and 3s
        sleep_time = random.uniform(1, 3)
        time.sleep(sleep_time)

# Close the Kafka producer
producer.close()
