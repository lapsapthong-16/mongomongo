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

# Load your CSV data
df = pd.read_csv('tweets_output_with_sentiment.csv')

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define function to categorize tweets based on keywords
def categorize_tweet(tweet_text):
    # Ensure tweet_text is a string
    if isinstance(tweet_text, str):  # Check if tweet_text is a string
        tweet_text = tweet_text.lower()  # Make the text case-insensitive
        for category, keywords in CATEGORY_KEYWORDS.items():
            if any(keyword in tweet_text for keyword in keywords):  # Check if any keyword matches
                return category
    return 'MalaysiaNewsTopic'  # Default topic if no category is found or invalid tweet

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

    # Print confirmation (optional)
    print(f"Sent tweet from {row['Name']} to {topic}")

# Close the producer
producer.flush()
producer.close()
print("All tweets have been sent to Kafka")
