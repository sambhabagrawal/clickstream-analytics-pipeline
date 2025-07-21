from kafka import KafkaProducer
import json
import time
import random

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define possible user actions
actions = ['click', 'scroll', 'purchase', 'navigate']

print("🚀 Kafka Producer started... Sending 50 events.\n")

# Send only 50 events
for i in range(50):
    data = {
        'user_id': random.randint(100, 110),
        'timestamp': time.time(),
        'action': random.choice(actions)
    }

    # Send to Kafka topic
    producer.send('user_clicks', value=data)
    print(f"📤 [{i+1}/50] Sent:", data)

    time.sleep(1)  # 1-second delay between messages

print("\n✅ Done sending 50 events.")
