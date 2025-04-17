#------------------------
# Author: Tan Zhi Wei
#------------------------
import json
from kafka import KafkaConsumer

def verify_kafka_topic(selected_topic, message_limit=5, timeout_ms=10000):
    """
    Verify a Kafka topic by consuming messages from it.
    
    Args:
        selected_topic (str): The Kafka topic to verify
        message_limit (int): Maximum number of messages to display
        timeout_ms (int): Consumer timeout in milliseconds
        
    Returns:
        int: Number of messages found in the topic
    """
    # Initialize Kafka consumer for the selected topic
    consumer = KafkaConsumer(
        selected_topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='verification_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=timeout_ms
    )
    
    print(f"Checking messages in {selected_topic}...")
    message_count = 0
    
    # Try to consume messages
    for message in consumer:
        message_count += 1
        print(f"Message {message_count}: {message.value}")
        
        # Optional: limit the number of messages to display
        if message_count >= message_limit:
            print(f"Showing first {message_limit} messages. Topic may contain more messages...")
            break
    
    # Close the consumer
    consumer.close()
    
    # Check if messages were found
    if message_count > 0:
        print(f"\n✅ Topic verification successful! Found {message_count} messages in {selected_topic}.")
    else:
        print(f"\n❌ No messages found in {selected_topic}. Please check your producer code or Kafka setup.")
    
    return message_count

def get_available_topics():
    """Returns a list of available Kafka topics."""
    return [
        'PoliticsNewsTopic',
        'BusinessNewsTopic',
        'SportsNewsTopic',
        'EntertainmentNewsTopic',
        'MalaysiaNewsTopic'
    ]

def display_available_topics(topics):
    """Display available topics with numbers."""
    print("Available Topics to verify:")
    for i, topic in enumerate(topics, 1):
        print(f"{i}. {topic}")

def get_topic_selection(topics):
    """Get user selection for a topic."""
    try:
        topic_choice = int(input("\nEnter the number of the topic you want to verify: "))
        if 1 <= topic_choice <= len(topics):
            return topics[topic_choice - 1]
        else:
            print("\n❌ Invalid selection. Please choose a valid topic number.")
            return None
    except ValueError:
        print("\n❌ Invalid input. Please enter a number.")
        return None

# Main code
if __name__ == "__main__":
    topics = get_available_topics()
    display_available_topics(topics)
    
    selected_topic = get_topic_selection(topics)
    if selected_topic:
        verify_kafka_topic(selected_topic)
