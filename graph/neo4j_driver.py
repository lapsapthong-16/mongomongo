# Author: Edwina Hon Kai Xin

from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_write(self, query, params=None):
        with self.driver.session() as session:
            return session.execute_write(lambda tx: tx.run(query, params or {}))

    def execute_read(self, query, params=None):
        with self.driver.session() as session:
            return session.execute_read(lambda tx: tx.run(query, params or {}))