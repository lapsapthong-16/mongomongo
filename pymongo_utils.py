from pymongo import MongoClient

# Replace with your connection string and password
CONNECTION_STRING = "mongodb+srv://lapsap:i3WlHcY2fKGs7TUf@cluster0.fs22oe8.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

class PyMongoUtils:
    
    def __init__(self, uri=CONNECTION_STRING):
        self.uri = uri    

    def get_database(self, database_name):
        client = MongoClient(self.uri)
        return client[database_name]
