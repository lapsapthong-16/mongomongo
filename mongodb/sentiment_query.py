# Author: Edwina Hon Kai Xin

from datetime import datetime

class sentiment_query:
    def __init__(self, tweets_collection, users_collection=None):
        self.tweets = tweets_collection
        self.users = users_collection

    def find_by_sentiment(self, sentiment):
        return list(self.tweets.find({"sentiment": sentiment}))

    def find_by_prediction(self, prediction_value):
        return list(self.tweets.find({"prediction": prediction_value}))

    def find_by_topic(self, topic):
        return list(self.tweets.find({"topic": topic}))

    def find_by_user_id(self, user_id):
        return list(self.tweets.find({"user_id": user_id}))

    def find_by_location(self, location_name):
        if self.users:
            return list(self.users.find({"location": location_name}))
        return []

    def find_by_name(self, name):
        if self.users:
            return list(self.users.find({"name": name}))
        return []

    def find_by_followers_min(self, min_followers):
        if self.users:
            return list(self.users.find({
                "followers_count": {"$gte": int(min_followers)}
            }))
        return []

    def find_by_date_range(self, start_date, end_date):
        # Convert input strings to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        # Fetch tweets where time feature exists
        result = []
        for doc in self.tweets.find({"time": {"$exists": True}}):
            try:
                tweet_time = doc["time"]  
                if start_dt <= tweet_time <= end_dt:
                    result.append(doc)
            except Exception as e:
                print(f"Failed to parse time for document: {doc.get('time')}, Error: {e}")

        return result

    def sentiment_over_time(self):
        pipeline = [
            {
                "$project": {
                    "sentiment": 1,
                    "date": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {"$toDate": "$time"}
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {"date": "$date", "sentiment": "$sentiment"},
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id.date": 1}}
        ]
        return list(self.tweets.aggregate(pipeline))

    def count_by_sentiment(self):
        pipeline = [
            {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        return list(self.tweets.aggregate(pipeline))

    def count_by_topic(self):
        pipeline = [
            {"$group": {"_id": "$topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        return list(self.tweets.aggregate(pipeline))

    def tweets_with_user_info(self):
        if not self.users:
            return []

        pipeline = [
            {
                "$lookup": {
                    "from": self.users.name if self.users else "users",
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "user_info"
                }
            },
            {"$unwind": "$user_info"}
        ]
        return list(self.tweets.aggregate(pipeline))

    def find_tweets_by_user_location(self, location_name):
        if not self.users:
            return []

        pipeline = [
            {
                "$lookup": {
                    "from": self.users.name, 
                    "localField": "user_id",
                    "foreignField": "user_id",
                    "as": "user_info"
                }
            },
            {"$unwind": "$user_info"},
            {"$match": {"user_info.location": location_name}}
        ]
        return list(self.tweets.aggregate(pipeline))
    
    def find_tweets_mentioning(self, keyword):
        return list(self.tweets.find({
            "tweet": {
                "$regex": keyword,
                "$options": "i"  # 'i' makes the search case-insensitive
            }
        }))
    
    def format_time_fields(self, docs, format_str="%Y-%m-%d %H:%M:%S"):
        for doc in docs:
            if isinstance(doc.get("time"), datetime):
                doc["time"] = doc["time"].strftime(format_str)
        return docs