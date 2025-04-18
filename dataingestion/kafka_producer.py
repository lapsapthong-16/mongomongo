#------------------------
# Author: Tan Zhi Wei
#------------------------
import pandas as pd
from kafka import KafkaProducer
import json
import os
from pathlib import Path

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
        tweet_text (str): The text of the tweet to categorize
        
    Returns:
        str: The category of the tweet (Force everything to 'MalaysiaNewsTopic')
    """
    # Ensure tweet_text is a string
    if isinstance(tweet_text, str):  # Check if tweet_text is a string
        tweet_text = tweet_text.lower()  # Make the text case-insensitive
        
        # Even though we check categories, we will return 'MalaysiaNewsTopic' no matter what
        for category, keywords in CATEGORY_KEYWORDS.items():
            if any(keyword.lower() in tweet_text for keyword in keywords):  # Check if any keyword matches
                # If a match is found, we ignore the category and return 'MalaysiaNewsTopic' only
                return 'MalaysiaNewsTopic'
    return 'MalaysiaNewsTopic'  # Default topic if no category is found or invalid tweet


def send_tweets_to_kafka(csv_file, bootstrap_servers=['localhost:9092'], verbose=False):
    """
    Process tweets from a CSV file and send them to the MalaysiaNewsTopic Kafka topic only.
    
    Args:
        csv_file (str): Path to the CSV file containing tweets
        bootstrap_servers (list): List of Kafka bootstrap servers
        verbose (bool): Whether to print processing information
        
    Returns:
        int: Number of tweets processed
    """
    # Load your CSV data
    df = pd.read_csv(csv_file)
    
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    tweet_count = 0
    
    # Iterate and send data to the MalaysiaNewsTopic Kafka topic
    for index, row in df.iterrows():
        tweet_data = {}
        
        # Map fields to columns from the CSV
        tweet_data['user_id'] = row['User ID']
        tweet_data['name'] = row['Name']
        tweet_data['followers_count'] = row['Followers Count']
        tweet_data['tweet_text'] = row['Tweet']
        tweet_data['location'] = row['Location']
        tweet_data['created_at'] = row['Time']
        tweet_data['friends_count'] = row['Friends Count']
        tweet_data['sentiment'] = row['Sentiment']
        
        # Force all tweets to go to MalaysiaNewsTopic
        topic = 'MalaysiaNewsTopic'  # Hardcoding the topic to MalaysiaNewsTopic
        
        # Send each tweet to the Kafka topic
        producer.send(topic, value=tweet_data)
        tweet_count += 1
        
        if verbose and tweet_count <= 3:  # Only show first 3 for debugging
            print(f"Tweet {tweet_count} data: {tweet_data}")
            print(f"Sent to topic: {topic}")
    
    # Close the producer
    producer.flush()
    producer.close()
    
    if verbose:
        print("All tweets have been sent to Kafka")
    
    return tweet_count


