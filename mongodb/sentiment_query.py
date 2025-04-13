# Author: Edwina Hon Kai Xin

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
        return list(self.collection.find({
            "Time": {"$gte": start_date, "$lte": end_date}
        }))

    # Top N sources by tweet count
    def top_sources(self, n=5):
        pipeline = [
            {"$group": {"_id": "$name", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": n}
        ]
        return list(self.collection.aggregate(pipeline))

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