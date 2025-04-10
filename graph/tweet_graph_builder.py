# Author: Your Name

class TweetGraphBuilder:
    def __init__(self, neo4j_conn):
        self.conn = neo4j_conn

    def create_tweet_graph(self, tweet, sentiment, source):
        query = """
        MERGE (s:Source {name: $source})
        MERGE (t:Tweet {text: $tweet})
        MERGE (sent:Sentiment {label: $sentiment})
        MERGE (s)-[:POSTED]->(t)
        MERGE (t)-[:HAS_SENTIMENT]->(sent)
        """
        self.conn.execute_write(query, {
            "tweet": tweet,
            "sentiment": sentiment,
            "source": source
        })