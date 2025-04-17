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
        str: The category of the tweet
    """
    # Ensure tweet_text is a string
    if isinstance(tweet_text, str):  # Check if tweet_text is a string
        tweet_text = tweet_text.lower()  # Make the text case-insensitive
        for category, keywords in CATEGORY_KEYWORDS.items():
            if any(keyword.lower() in tweet_text for keyword in keywords):  # Check if any keyword matches
                return category
    return 'MalaysiaNewsTopic'  # Default topic if no category is found or invalid tweet

def send_tweets_to_kafka(csv_file, bootstrap_servers=['localhost:9092'], verbose=False):
    """
    Process tweets from a CSV file and send them to appropriate Kafka topics.
    
    Args:
        csv_file (str): Path to the CSV file containing tweets
        bootstrap_servers (list): List of Kafka bootstrap servers
        verbose (bool): Whether to print processing information
        
    Returns:
        int: Number of tweets processed
    """
    # Load your CSV data
    df = pd.read_csv(csv_file)
    
    # Check and print column names for debugging
    if verbose:
        print("Available columns in CSV:", df.columns.tolist())
    
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    tweet_count = 0
    
    # Map of possible column name variations
    column_mapping = {
        'user_id': ['User ID', 'UserID', 'user_id', 'userId'],
        'name': ['Name', 'Username', 'user_name', 'UserName'],
        'followers_count': ['Followers Count', 'FollowersCount', 'followers_count'],
        'tweet_text': ['Tweet', 'TweetText', 'tweet_text', 'Text'],
        'location': ['Location', 'UserLocation', 'user_location'],
        'created_at': ['TweetTime', 'Tweet Time', 'Time', 'Created At', 'created_at'],
        'friends_count': ['FriendsCount', 'Friends Count', 'friends_count'],
        'sentiment': ['Sentiment', 'sentiment_label', 'SentimentScore']
    }
    
    # Find actual column names once (not for each row)
    field_to_column = {}
    for field, possible_names in column_mapping.items():
        for name in possible_names:
            if name in df.columns:
                field_to_column[field] = name
                break
        
        if field not in field_to_column and verbose:
            print(f"Warning: Could not find column for '{field}'")
    
    if verbose:
        print("Mapped fields to actual columns:", field_to_column)
    
    # Iterate and send data to the appropriate Kafka topic
    for index, row in df.iterrows():
        tweet_data = {}
        
        # Build tweet_data using the field mapping we established
        for field, column_name in field_to_column.items():
            tweet_data[field] = row[column_name]
        
        # Categorize the tweet into the correct topic
        tweet_text = tweet_data.get('tweet_text')
        topic = categorize_tweet(tweet_text)
        
        # Send each row to the appropriate Kafka topic
        producer.send(topic, value=tweet_data)
        tweet_count += 1
        
        # Print confirmation if verbose is enabled
        if verbose and tweet_count <= 3:  # Only show first 3 to avoid spam
            print(f"Tweet {tweet_count} data: {tweet_data}")
            print(f"Sent to topic: {topic}")
    
    # Close the producer
    producer.flush()
    producer.close()
    
    if verbose:
        print("All tweets have been sent to Kafka")
    
    return tweet_count

