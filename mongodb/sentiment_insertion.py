# Author: Edwina Hon Kai Xin
from datetime import datetime

class sentiment_insertion:
    def __init__(self, collection):
        self.collection = collection

    @staticmethod
    def preprocess_data(data):
        for tweet in data:
            if "followers_count" in tweet and isinstance(tweet["followers_count"], str):
                try:
                    tweet["followers_count"] = int(tweet["followers_count"].replace(",", ""))
                except ValueError:
                    tweet["followers_count"] = 0  
            
            if "Time" in tweet and isinstance(tweet["Time"], str):
                try:
                    tweet["Time"] = datetime.strptime(tweet["Time"], "%a %b %d %H:%M:%S %z %Y")
                except ValueError:
                    pass
            elif "time" in tweet and isinstance(tweet["time"], str):
                try:
                    tweet["time"] = datetime.strptime(tweet["time"], "%a %b %d %H:%M:%S %z %Y")
                except ValueError:
                    pass
        return data
    
    @staticmethod
    def normalize_field_names(data):
        """Convert field names to lowercase"""
        normalized_data = []
        for tweet in data:
            normalized_tweet = {}
            for key, value in tweet.items():
                # Convert keys like 'Tweet' to 'tweet' and 'Time' to 'time' for consistency
                normalized_key = key.lower()
                normalized_tweet[normalized_key] = value
            normalized_data.append(normalized_tweet)
        return normalized_data
    
    @staticmethod
    def extract_users(data):
        """Extract unique users from the data"""
        users = {}
        for tweet in data:
            user_id = tweet.get("user_id")
            if user_id and user_id not in users:
                users[user_id] = {
                    "user_id": user_id,
                    "name": tweet.get("name", ""),
                    "followers_count": tweet.get("followers_count", 0),
                    "location": tweet.get("location", "")  # Now using lowercase 'location'
                }
        return list(users.values())
    
    @staticmethod
    def remove_user_fields(data):
        """Remove user fields from tweets"""
        modified_tweets = []
        for tweet in data:
            tweet_copy = tweet.copy()
            # Remove user fields - now using lowercase field names
            for field in ["name", "followers_count", "location"]:
                if field in tweet_copy:
                    del tweet_copy[field]
            modified_tweets.append(tweet_copy)
        return modified_tweets

    def insert_many(self, data):
        if isinstance(data, list):
            self.collection.insert_many(data)
        else:
            raise ValueError("Data must be a list of dictionaries")

    def insert_one(self, record):
        self.collection.insert_one(record)
