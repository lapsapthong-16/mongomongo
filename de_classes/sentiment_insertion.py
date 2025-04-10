# Author: Edwina Hon Kai Xin

class sentiment_insertion:
    def __init__(self, collection):
        self.collection = collection

    def insert_many(self, data):
        if isinstance(data, list):
            self.collection.insert_many(data)
        else:
            raise ValueError("Data must be a list of dictionaries")

    def insert_one(self, record):
        self.collection.insert_one(record)
