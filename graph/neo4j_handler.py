# Author: Edwina Hon Kai Xin

from neo4j import GraphDatabase

CONNECTION_STRING = "neo4j+s://2f87e004.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "g-c66iZIlb7VPDfb0c6Z6yAYEgbh2OXOytSReU7gqIk"
            
class Neo4jHandler:
    def __init__(self, uri = CONNECTION_STRING, user = USER, password = PASSWORD):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def insert_tweet_graph(self, tweet_text, sentiment, source, entities):
        with self.driver.session() as session:
            session.write_transaction(
                self._create_graph, tweet_text, sentiment, source, entities
            )

    @staticmethod
    def _create_graph(tx, tweet_text, sentiment, source, entities):
        query = """
        MERGE (t:Tweet {text: $tweet_text, sentiment: $sentiment})
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
               topics=entities["topics"],
               locations=entities["locations"],
               people=entities["people"],
               orgs=entities["organizations"])
        