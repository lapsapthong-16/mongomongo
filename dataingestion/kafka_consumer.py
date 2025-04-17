#------------------------
# Author: Tan Zhi Wei
#------------------------
import json
from kafka import KafkaConsumer
import pandas as pd

def consume_kafka_messages(selected_topic, message_limit=5, timeout_ms=10000, output_format="detailed"):
    """
    Consume messages from a Kafka topic and display all fields.
    
    Args:
        selected_topic (str): The Kafka topic to consume from
        message_limit (int): Maximum number of messages to display
        timeout_ms (int): Consumer timeout in milliseconds
        output_format (str): Format to display messages ("detailed", "table", or "compact")
        
    Returns:
        list: The consumed messages
    """
    # Initialize Kafka consumer for the selected topic
    consumer = KafkaConsumer(
        selected_topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id='tweet_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=timeout_ms
    )
    
    print(f"Consuming messages from {selected_topic}...")
    message_count = 0
    messages = []
    
    # Keep consuming messages indefinitely
    while True:
        for message in consumer:
            message_count += 1
            message_value = message.value
            messages.append(message_value)
            
            # Display message based on format
            if output_format == "detailed":
                print(f"\nMessage {message_count}:")
                for field, value in message_value.items():
                    print(f"  {field}: {value}")
            elif output_format == "table" and message_count == 1:
                # Start collecting for table format, show table after all messages
                table_data = []
                headers = list(message_value.keys())
                row = list(message_value.values())
                table_data.append(row)
                # Continue with compact display for now
                print(f"Message {message_count}: {message_value}")
            elif output_format == "table":
                # Add to table data
                row = [message_value.get(field, "") for field in headers]
                table_data.append(row)
                # Continue with compact display for now
                print(f"Message {message_count}: {message_value}")
            else:  # "compact" format
                print(f"Message {message_count}: {message_value}")
            
            # Optional: limit the number of messages to display
            if message_count >= message_limit:
                remaining = sum(1 for _ in consumer)
                print(f"\nShowing first {message_limit} messages. {remaining} more messages available.")
                break
    
        # Continue consuming
        if message_count >= message_limit:
            break
    
    # Check if messages were found
    if message_count > 0:
        print(f"\n✅ Successfully consumed {message_count} messages from {selected_topic}.")
        
        # Show field statistics
        all_fields = set()
        for msg in messages:
            all_fields.update(msg.keys())
        
        print("\n=== Fields Found in Messages ===")
        for field in sorted(all_fields):
            count = sum(1 for msg in messages if field in msg)
            percentage = (count / len(messages)) * 100
            print(f"  - {field}: Present in {count}/{len(messages)} messages ({percentage:.1f}%)")
    else:
        print(f"\n❌ No messages found in {selected_topic}. Please check your producer or Kafka setup.")
    
    return messages

def get_available_topics():
    """Returns a list of available Kafka topics based on the producer categories."""
    return [
        'PoliticsNewsTopic',
        'BusinessNewsTopic',
        'SportsNewsTopic',
        'EntertainmentNewsTopic',
        'MalaysiaNewsTopic'
    ]

def display_available_topics(topics):
    """Display available topics with numbers."""
    print("Available Topics:")
    for i, topic in enumerate(topics, 1):
        print(f"{i}. {topic}")

def get_topic_selection(topics):
    """Get user selection for a topic."""
    try:
        topic_choice = int(input("\nEnter the number of the topic you want to consume from: "))
        if 1 <= topic_choice <= len(topics):
            return topics[topic_choice - 1]
        else:
            print("\n❌ Invalid selection. Please choose a valid topic number.")
            return None
    except ValueError:
        print("\n❌ Invalid input. Please enter a number.")
        return None

def select_output_format():
    """Let user select an output format."""
    print("\nOutput Format Options:")
    print("1. Detailed - Show each field on a separate line")
    print("2. Table - Show messages in a table format")
    print("3. Compact - Show messages in a single line each")
    
    try:
        format_choice = int(input("\nSelect output format (1-3): "))
        if format_choice == 1:
            return "detailed"
        elif format_choice == 2:
            return "table"
        elif format_choice == 3:
            return "compact"
        else:
            print("Invalid choice, using detailed format.")
            return "detailed"
    except ValueError:
        print("Invalid input, using detailed format.")
        return "detailed"

def export_to_csv(messages, filename="consumed_tweets.csv"):
    """Export consumed messages to a CSV file."""
    if not messages:
        print("No messages to export.")
        return False
    
    try:
        df = pd.DataFrame(messages)
        df.to_csv(filename, index=False)
        print(f"\n✅ Successfully exported {len(messages)} messages to {filename}")
        return True
    except Exception as e:
        print(f"\n❌ Error exporting to CSV: {e}")
        return False

def run_kafka_consumer():
    """Main function to run the Kafka consumer."""
    print("#----------------------------------------#")
    print("#   Kafka Tweet Consumer Tool            #")
    print("#----------------------------------------#")
    
    # Get and display available topics
    topics = get_available_topics()
    display_available_topics(topics)
    
    # Let user select a topic
    selected_topic = get_topic_selection(topics)
    if not selected_topic:
        return
    
    # Let user select output format
    output_format = select_output_format()
    
    # Get message limit
    try:
        message_limit = int(input("\nEnter maximum number of messages to display (default: 5): ") or "5")
    except ValueError:
        message_limit = 5
        print("Invalid input, using default value of 5 messages.")
    
    # Consume messages
    messages = consume_kafka_messages(
        selected_topic=selected_topic,
        message_limit=message_limit,
        output_format=output_format
    )
    
    # Ask if user wants to export messages
    if messages:
        export_choice = input("\nDo you want to export these messages to CSV? (y/n): ").lower()
        if export_choice.startswith('y'):
            filename = input("Enter filename (default: consumed_tweets.csv): ") or "consumed_tweets.csv"
            export_to_csv(messages, filename)
    
    print("\nKafka consumer operation completed.")
