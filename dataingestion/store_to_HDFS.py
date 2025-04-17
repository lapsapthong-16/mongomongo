#------------------------
# Author: Tan Zhi Wei
#------------------------
import json
import datetime
import uuid
from kafka import KafkaConsumer
from hdfs import InsecureClient

def write_to_hdfs(data, hdfs_client, hdfs_directory):
    try:
        # Generate unique filename with timestamp and UUID
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]  # Use first 8 chars of UUID for brevity
        file_name = f"tweets_raw_{timestamp}_{unique_id}.json"
        
        # Full path to HDFS
        hdfs_path = f"{hdfs_directory}/{file_name}"
        
        # Convert the data to JSON
        data_str = json.dumps(data, ensure_ascii=False)
        
        # Write the data to HDFS
        with hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
            writer.write(data_str)
            
        print(f"Successfully wrote to HDFS: {hdfs_path}")
        return True
    except Exception as e:
        print(f"Error writing to HDFS: {e}")
        return False

def write_batch_to_hdfs(batch_data, hdfs_client, hdfs_directory, batch_size=10):
    if not batch_data:
        return True
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    file_name = f"tweets_batch_{timestamp}_{unique_id}_{batch_size}.json"
    hdfs_path = f"{hdfs_directory}/{file_name}"
    
    data_str = json.dumps(batch_data, ensure_ascii=False)
    
    with hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
        writer.write(data_str)
    
    print(f"Successfully wrote batch of {len(batch_data)} tweets to HDFS")
    return True

def consume_tweets_to_hdfs():
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        'MalaysiaNewsTopic',  # Topic name
        bootstrap_servers=['localhost:9092'],
        group_id='tweet_consumer_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Initialize HDFS client
    hdfs_client = InsecureClient('http://localhost:9870', user='hduser')
    
    # Specify the HDFS directory
    hdfs_directory = '/user/hduser/raw_tweets/'
    
    try:
        # For batch processing
        batch = []
        batch_size = 10
        
        for message in consumer:
            tweet_data = message.value
            
            # Ensure tweet data contains the fields we need
            tweet_data = {
                'user_id': tweet_data['user_id'],
                'name': tweet_data['name'],
                'followers_count': tweet_data['followers_count'],
                'tweet_text': tweet_data['tweet_text'],
                'sentiment': tweet_data.get('sentiment', 'Unknown')  # Include sentiment if available
            }
            
            print(f"Received tweet data: {tweet_data}")
            
            # Write individual tweet data to HDFS
            write_to_hdfs(tweet_data, hdfs_client, hdfs_directory)
            
            # Batch processing logic
            batch.append(tweet_data)
            if len(batch) >= batch_size:
                write_batch_to_hdfs(batch, hdfs_client, hdfs_directory, batch_size)
                batch = []  # Reset batch after writing
        
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    consume_tweets_to_hdfs()
