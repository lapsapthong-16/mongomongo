import json
import datetime
import time
from kafka import KafkaConsumer
from hdfs import InsecureClient

def consume_tweets_to_hdfs(topic='MalaysiaNewsTopic', bootstrap_servers=['localhost:9092'], hdfs_directory='/user/hduser/raw_tweets', limit=300):
    """
    Consume tweets from Kafka and store them directly to HDFS.
    Ensures all 8 features from tweets_output_with_sentiment.csv are included.
    
    :param topic: Kafka topic to consume tweets from (default: 'MalaysiaNewsTopic')
    :param bootstrap_servers: List of Kafka bootstrap servers (default: ['localhost:9092'])
    :param hdfs_directory: HDFS directory to store the tweets (default: '/user/hduser/raw_tweets')
    :param limit: Limit on the number of tweets to consume (default: 300)
    """
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        topic,  # Topic name
        bootstrap_servers=bootstrap_servers,
        group_id='tweet_hdfs_consumer',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    # Initialize HDFS client
    hdfs_client = InsecureClient('http://localhost:9870', user='hduser')
    
    # Make sure the directory exists
    try:
        if not hdfs_client.status(hdfs_directory, strict=False):
            hdfs_client.makedirs(hdfs_directory)
            print(f"Created HDFS directory: {hdfs_directory}")
    except Exception as e:
        print(f"Error checking/creating directory: {e}")
    
    # Generate timestamp once for the filename
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    hdfs_filename = f"{hdfs_directory}/tweets_{timestamp}.json"
    
    print(f"Starting to consume tweets and save to {hdfs_filename}")
    
    tweet_count = 0
    try:
        # Open the file once for appending all tweets
        with hdfs_client.write(hdfs_filename, encoding='utf-8') as writer:
            writer.write("[\n")  # Start JSON array
            
            first_tweet = True
            for message in consumer:
                try:
                    tweet_data = message.value
                    
                    # Ensure all 8 features are included from tweets_output_with_sentiment.csv
                    tweet_json = {
                        'user_id': tweet_data.get('user_id', ''),
                        'name': tweet_data.get('name', ''),
                        'followers_count': tweet_data.get('followers_count', 0),
                        'tweet_text': tweet_data.get('tweet_text', ''),
                        'location': tweet_data.get('location', ''),
                        'created_at': tweet_data.get('created_at', ''),
                        'friends_count': tweet_data.get('friends_count', 0),
                        'sentiment': tweet_data.get('sentiment', 'Unknown')
                    }
                    
                    # Add comma between tweets (not before the first one)
                    if not first_tweet:
                        writer.write(",\n")
                    else:
                        first_tweet = False
                    
                    # Write the tweet as a JSON object
                    json_str = json.dumps(tweet_json, ensure_ascii=False)
                    writer.write(json_str)
                    
                    tweet_count += 1
                    
                    # Print progress every 10 tweets
                    if tweet_count % 10 == 0:
                        print(f"Saved {tweet_count} tweets to HDFS")
                    
                    # Optional: Add a small pause to avoid overwhelming resources
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"Error processing tweet: {e}")
                    continue
                    
                # Stop after reaching the limit
                if tweet_count >= limit:
                    print(f"Reached limit of {limit} tweets")
                    break
                    
            writer.write("\n]")  # End JSON array
            
    except KeyboardInterrupt:
        print("\nStopping consumer due to keyboard interrupt...")
    except Exception as e:
        print(f"Error in main consumer loop: {e}")
    finally:
        consumer.close()
        print(f"Consumer closed. Saved {tweet_count} tweets to HDFS.")
        
        # Verify the file exists
        try:
            if hdfs_client.status(hdfs_filename, strict=False):
                print(f"Successfully verified file exists: {hdfs_filename}")
                print(f"Run 'hadoop fs -cat {hdfs_filename}' to view the content")
                print(f"To check structure: hadoop fs -cat {hdfs_filename} | head -20")
        except Exception as e:
            print(f"Error verifying file: {e}")
