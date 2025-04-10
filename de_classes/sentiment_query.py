
class sentiment_query:
    def __init__(self, collection):
        self.collection = collection

    def get_sentiment_distribution(self):
        return list(self.collection.aggregate([
            {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}
        ]))

    def get_top_negative_influencers(self, limit=5):
        return list(self.collection.find({"sentiment": "Negative"}).sort("followers_count", -1).limit(limit))

    def get_tweets_by_user(self, user_id):
        return list(self.collection.find({"user_id": user_id}))
