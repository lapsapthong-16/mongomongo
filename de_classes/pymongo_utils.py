from pymongo import MongoClient

# Replace with your connection string and password
CONNECTION_STRING = "mongodb+srv://lapsap:i3WlHcY2fKGs7TUf@cluster0.fs22oe8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

from pymongo import MongoClient

class PyMongoUtils:
    def __init__(self, uri=CONNECTION_STRING):
        self.client = MongoClient(uri)

    def get_database(self, db_name):
        return self.client[db_name]

    def get_collection(self, db_name, collection_name):
        return self.client[db_name][collection_name]

