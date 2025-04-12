# Author: Edwina Hon Kai Xin

class sentiment_query:
    def __init__(self, collection):
        self.collection = collection

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