# Author: Edwina Hon Kai Xin

from neo4j import GraphDatabase

CONNECTION_STRING = "neo4j+s://2f87e004.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "g-c66iZIlb7VPDfb0c6Z6yAYEgbh2OXOytSReU7gqIk"

class Neo4jConnection:
    def __init__(self, uri=CONNECTION_STRING, user=USER, password=PASSWORD):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_write(self, query, params=None):
        with self.driver.session() as session:
            return session.execute_write(lambda tx: tx.run(query, params or {}))

    def execute_read(self, query, params=None):
        with self.driver.session() as session:
            return session.execute_read(lambda tx: tx.run(query, params or {}))
        
        