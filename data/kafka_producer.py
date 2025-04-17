#------------------------
# Author: Tan Zhi Wei
#------------------------
import pandas as pd
from kafka import KafkaProducer
import json

# Define CATEGORY_KEYWORDS dictionary
CATEGORY_KEYWORDS = {
    'PoliticsNewsTopic': [
        'parliament', 'minister', 'government', 'election', 'policy', 'vote', 
        'cabinet', 'PM', 'democracy', 'corruption', 'political', 'politician',
        'law', 'bill', 'constitution', 'amendment', 'opposition', 'campaign',
        'UMNO', 'PAS', 'PKR', 'DAP', 'Bersatu', 'Pakatan', 'Barisan', 'budget'
    ],
    'BusinessNewsTopic': [
        'economy', 'market', 'stock', 'investment', 'company', 'business', 
        'trade', 'finance', 'bank', 'ringgit', 'profit', 'revenue', 'CEO',
        'entrepreneur', 'startup', 'commerce', 'industry', 'economic', 
        'inflation', 'recession', 'growth', 'GST', 'tax', 'BURSA', 'FDI'
    ],
    'SportsNewsTopic': [
        'football', 'badminton', 'hockey', 'athlete', 'tournament', 'championship',
        'league', 'match', 'player', 'coach', 'team', 'sport', 'medal', 'win',
        'game', 'score', 'FIFA', 'Olympic', 'Petronas', 'stadium', 'final',
        'competition', 'record', 'JDT', 'Selangor', 'Perak', 'Malaysia Super League'
    ],
    'EntertainmentNewsTopic': [
        'movie', 'music', 'concert', 'celebrity', 'actor', 'actress', 'film',
        'entertainment', 'drama', 'show', 'artist', 'singer', 'star', 'TV',
        'Netflix', 'performance', 'premiere', 'award', 'festival', 'viral',
        'album', 'song', 'talent', 'meme', 'trending', 'Astro', 'Media Prima'
    ]
}

def categorize_tweet(tweet_text):
    """
    Categorize a tweet based on keywords present in the text.
    
    Args:
        tweet_text (str): The text content of the tweet
        
    Returns:
        str: The category topic name
    """
    # Ensure tweet_text is a string
    if isinstance(tweet_text, str):  # Check if tweet_text is a string
        tweet_text = tweet_text.lower()  # Make the text case-insensitive
        for category, keywords in CATEGORY_KEYWORDS.items():
            if any(keyword.lower() in tweet_text for keyword in keywords):
                return category
    return 'MalaysiaNewsTopic'  # Default topic if no category is found or invalid tweet

def send_tweets_to_kafka(csv_file, bootstrap_servers=['localhost:9092'], verbose=True):
    """
    Process tweets from a CSV file and send them to appropriate Kafka topics.
    
    Args:
        csv_file (str): Path to the CSV file containing tweet data
        bootstrap_servers (list): List of Kafka bootstrap servers
        verbose (bool): Whether to print confirmation messages
        
    Returns:
        int: Number of tweets processed and sent
    """
    try:
        # Load CSV data
        df = pd.read_csv(csv_file)
        
        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Counter for processed tweets
        processed_count = 0
        
        # Iterate and send data to the appropriate Kafka topic
        for index, row in df.iterrows():
            tweet_data = {
                'user_id': row['User ID'],
                'name': row['Name'],
                'followers_count': row['Followers Count'],
                'tweet_text': row['Tweet'],
                'sentiment': row['Sentiment']
            }
            
            # Categorize the tweet into the correct topic
            topic = categorize_tweet(row['Tweet'])
            
            # Send each row to the appropriate Kafka topic
            producer.send(topic, value=tweet_data)
            processed_count += 1
            
            # Print confirmation if verbose mode is enabled
            if verbose:
                print(f"Sent tweet from {row['Name']} to {topic}")
        
        # Close the producer
        producer.flush()
        producer.close()
        
        if verbose:
            print(f"All {processed_count} tweets have been sent to Kafka")
        
        return processed_count
    
    except Exception as e:
        print(f"Error processing tweets: {e}")
        return 0
