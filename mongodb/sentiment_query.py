# Author: Edwina Hon Kai Xin
from datetime import datetime
class sentiment_query:
    def __init__(self, collection):
        self.collection = collection

    # Existing queries
    def find_by_sentiment(self, sentiment):
        return list(self.collection.find({"sentiment": sentiment}))

    def find_by_prediction(self, prediction_value):
        return list(self.collection.find({"prediction": prediction_value}))

    def find_by_source(self, source_name):
        return list(self.collection.find({"name": source_name}))

    def search_tweets(self, keyword):
        return list(self.collection.find({"Tweet": {"$regex": keyword, "$options": "i"}}))

    def count_by_sentiment(self):
        pipeline = [
            {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        return list(self.collection.aggregate(pipeline))

    def count_by_source(self):
        pipeline = [
            {"$group": {"_id": "$name", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        return list(self.collection.aggregate(pipeline))

    def count_by_sentiment_and_source(self):
        pipeline = [
            {"$group": {
                "_id": {"sentiment": "$sentiment", "source": "$name"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        return list(self.collection.aggregate(pipeline))

    # Tweets from a specific country/location
    def find_by_location(self, location_name):
        return list(self.collection.find({"Location": location_name}))

    # Tweets within a date range (based on ISO date string in "Time")
    def find_by_date_range(self, start_date, end_date):
        # Convert input strings to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Fetch all tweets
        all_docs = list(self.collection.find({"Time": {"$exists": True}}))

        # Filter in Python
        result = []
        for doc in all_docs:
            try:
                tweet_time = datetime.strptime(doc["Time"], "%a %b %d %H:%M:%S %z %Y")
                # Remove tzinfo for comparison with naive start/end_dt
                tweet_time_naive = tweet_time.replace(tzinfo=None)
                if start_dt <= tweet_time_naive <= end_dt:
                    result.append(doc)
            except Exception as e:
                print(f"Failed to parse date for document: {doc.get('Time')}, Error: {e}")

        return result

    # Sentiment distribution over time
    def sentiment_over_time(self):
        pipeline = [
            {"$project": {
                "sentiment": 1,
                "date": {"$substr": ["$Time", 0, 10]}  # Extract date part
            }},
            {"$group": {
                "_id": {"date": "$date", "sentiment": "$sentiment"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id.date": 1}}
        ]
        return list(self.collection.aggregate(pipeline))

    # Get tweets by followers count threshold (e.g. popular sources)
    def find_by_followers_min(self, min_followers):
        return list(self.collection.find({
            "followers_count": {"$gte": str(min_followers)}
        }))

    # Count by topic
    def count_by_topic(self):
        pipeline = [
            {"$group": {"_id": "$topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        return list(self.collection.aggregate(pipeline))

    def find_by_topic(self, topic):
        return list(self.collection.find({"topic": topic}))