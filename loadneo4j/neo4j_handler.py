# Author: Edwina Hon Kai Xin

from neo4j import GraphDatabase
import os

class Neo4jHandler:
    def __init__(self, uri=os.getenv("NEO4J_URI"), user=os.getenv("NEO4J_USER"), password=os.getenv("NEO4J_PASSWORD")):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def insert_tweet_graph(self, tweet_text, sentiment, source, time, entities):
        with self.driver.session() as session:
            session.execute_write(
                self._create_graph,
                tweet_text, sentiment, source, time, entities
            )

    @staticmethod
    def _create_graph(tx, tweet_text, sentiment, source, time, entities):
        query = """
        MERGE (t:Tweet {text: $tweet_text})
        SET t.sentiment = $sentiment,
            t.time = $time

        MERGE (s:Source {name: $source})
        MERGE (t)-[:PUBLISHED_BY]->(s)

        FOREACH (topic IN $topics |
            MERGE (tp:Topic {name: topic})
            MERGE (t)-[:TAGGED_AS]->(tp)
        )

        FOREACH (loc IN $locations |
            MERGE (l:Location {name: loc})
            MERGE (t)-[:ABOUT]->(l)
        )

        FOREACH (person IN $people |
            MERGE (p:Person {name: person})
            MERGE (t)-[:MENTIONS]->(p)
        )

        FOREACH (org IN $orgs |
            MERGE (o:Organization {name: org})
            MERGE (t)-[:REFERENCES]->(o)
        )
        """
        tx.run(query,
               tweet_text=tweet_text,
               sentiment=sentiment,
               source=source,
               time=time,
               topics=entities["topics"],
               locations=entities["locations"],
               people=entities["people"],
               orgs=entities["organizations"])