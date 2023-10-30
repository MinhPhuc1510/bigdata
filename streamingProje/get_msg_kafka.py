from kafka import KafkaConsumer

# Kafka broker address and topic name
bootstrap_servers = 'localhost:9094'  # Replace with your Kafka broker address
topic = 'iot_02'  # Replace with your Kafka topic name

# Create a Kafka consumer instance
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)

# create Kafka consumer instance
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')
# poll for new messages in the topic and print them to the console
for message in consumer:
    print(message.value.decode())