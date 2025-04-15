# Author: Edwina Hon Kai Xin

class sentiment_insertion:
    def __init__(self, collection):
        self.collection = collection

    def preprocess_data(data):
        for tweet in data:
            if "followers_count" in tweet and isinstance(tweet["followers_count"], str):
                try:
                    tweet["followers_count"] = int(tweet["followers_count"].replace(",", ""))
                except ValueError:
                    tweet["followers_count"] = 0  
        return data

    def insert_many(self, data):
        if isinstance(data, list):
            self.collection.insert_many(data)
        else:
            raise ValueError("Data must be a list of dictionaries")

    def insert_one(self, record):
        self.collection.insert_one(record)
