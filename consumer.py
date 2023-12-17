from kafka import KafkaConsumer

# Kafka configuration
bootstrap_servers = 'your_kafka_bootstrap_servers'
topic_name = 'your_kafka_topic'

# Create a Kafka consumer
consumer = KafkaConsumer(topic_name, bootstrap_servers=bootstrap_servers, group_id='your_consumer_group_id')

# Poll for new messages indefinitely
for message in consumer:
    # Process the received message
    print(f"Received message: {message.value}")
