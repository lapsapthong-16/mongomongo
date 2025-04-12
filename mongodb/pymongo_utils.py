# Author: Edwina Hon Kai Xin

from pymongo import MongoClient
import os

CONNECTION_STRING = os.getenv("MONGO_URI")

class PyMongoUtils:
    def __init__(self, uri=CONNECTION_STRING):
        self.client = MongoClient(uri)

    def get_database(self, db_name):
        return self.client[db_name]

    def get_collection(self, db_name, collection_name):
        return self.client[db_name][collection_name]
    