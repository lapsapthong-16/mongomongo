#------------------------
# Author: Tan Zhi Wei
#------------------------

import json
from kafka import KafkaConsumer

# Function to consume messages from the selected topic
def consume_messages_from_topic(selected_topic):
    # Initialize Kafka consumer for the selected topic
    consumer = KafkaConsumer(
        selected_topic,  # The topic to verify
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Start from the beginning of the topic
        group_id='verification_group',  # A unique consumer group for this verification
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # Exit after 10 seconds of no new messages
    )

    print(f"Checking messages in {selected_topic}...")
    message_count = 0

    # Try to consume messages
    for message in consumer:
        message_count += 1
        print(f"Message {message_count}: {message.value}")
        
        # Optional: limit the number of messages to display
        if message_count >= 5:
            print(f"Showing first 5 messages. Topic contains more messages...")
            break

    # Close the consumer
    consumer.close()

    # Check if messages were found
    if message_count > 0:
        print(f"\n✅ Topic verification successful! Found {message_count} messages in {selected_topic}.")
    else:
        print(f"\n❌ No messages found in {selected_topic}. Please check your producer code or Kafka setup.")

# Allow user to choose the topic
available_topics = [
    'PoliticsNewsTopic',
    'BusinessNewsTopic',
    'SportsNewsTopic',
    'EntertainmentNewsTopic',
    'MalaysiaNewsTopic'
]

print("Available Topics to verify:")
for i, topic in enumerate(available_topics, 1):
    print(f"{i}. {topic}")

# Ask the user to select a topic
try:
    topic_choice = int(input("\nEnter the number of the topic you want to verify: "))
    if 1 <= topic_choice <= len(available_topics):
        selected_topic = available_topics[topic_choice - 1]
        consume_messages_from_topic(selected_topic)
    else:
        print("\n❌ Invalid selection. Please choose a valid topic number.")
except ValueError:
    print("\n❌ Invalid input. Please enter a number.")

