# Author: Edwina Hon Kai Xin

class tweet_graph:
    def __init__(self, neo4j_handler):
        self.driver = neo4j_handler.driver

    def get_tweets_mentioning_person(self, person_name):
        query = """
        MATCH (p:Person {name: $person_name})<-[:MENTIONS]-(t:Tweet)
        RETURN t.text AS tweet, t.time AS time
        """
        return self._run_query(query, {"person_name": person_name})

    def get_sentiment_by_source(self, source_name):
        query = """
        MATCH (s:Source {name: $source_name})-[:PUBLISHED]->(t:Tweet)-[:HAS_SENTIMENT]->(sent:Sentiment)
        RETURN sent.label AS sentiment, COUNT(*) AS count
        ORDER BY count DESC
        """
        return self._run_query(query, {"source_name": source_name})

    def get_tweets_about_location(self, location_name):
        query = """
        MATCH (l:Location {name: $location_name})<-[:ABOUT]-(t:Tweet)
        RETURN t.text AS tweet, t.time AS time
        """
        return self._run_query(query, {"location_name": location_name})

    def get_top_topics(self, limit=5):
        query = """
        MATCH (t:Tweet)-[:TAGGED_AS]->(topic:Topic)
        RETURN topic.name AS topic, COUNT(*) AS count
        ORDER BY count DESC
        LIMIT $limit
        """
        return self._run_query(query, {"limit": limit})

    def get_tweets_by_sentiment(self, sentiment_label):
        query = """
        MATCH (t:Tweet)-[:HAS_SENTIMENT]->(s:Sentiment {label: $sentiment_label})
        RETURN t.text AS tweet, t.time AS time
        """
        return self._run_query(query, {"sentiment_label": sentiment_label})

    def get_tweets_by_source_and_date(self, source_name, start_date, end_date):
        query = """
        MATCH (s:Source {name: $source_name})-[:PUBLISHED]->(t:Tweet)
        WHERE t.time >= datetime($start_date) AND t.time <= datetime($end_date)
        RETURN t.text AS tweet, t.time AS time
        ORDER BY t.time ASC
        """
        return self._run_query(query, {
            "source_name": source_name,
            "start_date": start_date,
            "end_date": end_date
        })

    def get_most_referenced_organisations(self, limit=5):
        query = """
        MATCH (t:Tweet)-[:REFERENCES]->(o:Organization)
        RETURN o.name AS organisation, COUNT(*) AS references
        ORDER BY references DESC
        LIMIT $limit
        """
        return self._run_query(query, {"limit": limit})

    def get_tweets_by_topic_and_location(self, topic_name, location_name):
        query = """
        MATCH (t:Tweet)-[:TAGGED_AS]->(topic:Topic {name: $topic_name}),
              (t)-[:ABOUT]->(location:Location {name: $location_name})
        RETURN t.text AS tweet, t.time AS time
        """
        return self._run_query(query, {
            "topic_name": topic_name,
            "location_name": location_name
        })

    def sentiment_distribution(self):
        query = """
        MATCH (t:Tweet)-[:HAS_SENTIMENT]->(s:Sentiment)
        RETURN s.label AS sentiment, COUNT(*) AS count
        ORDER BY count DESC
        """
        return self._run_query(query)

    def _run_query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]