from kafka import KafkaConsumer
import json
import mysql.connector

# Step 1: Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="sambhab@1234",  # ✅ Replace this!
    database="clickstream"
)

cursor = db.cursor()

# Step 2: Setup Kafka Consumer
consumer = KafkaConsumer(
    'user_clicks',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',              # <-- Important to read old messages
    group_id='my-consumer-group',              # <-- Same group reads from last point
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("📡 Listening for messages on 'user_clicks'...")

# Step 3: Loop and process messages
for message in consumer:
    try:
        data = message.value
        print("📩 Received from Kafka:", data)

        # Insert into MySQL
        sql = "INSERT INTO user_clicks (user_id, timestamp, action) VALUES (%s, %s, %s)"
        val = (data['user_id'], data['timestamp'], data['action'])

        cursor.execute(sql, val)
        db.commit()
        print("✅ Inserted into MySQL:", val)

    except Exception as e:
        print("❌ Error:", e)
