# Class to validate tweets
class TweetValidator:
    def __init__(self, required_fields=None, allowed_sentiments=None):
        # Define required fields
        self.required_fields = required_fields or ["Tweet", "sentiment", "name", "prediction"]
        # Define allowed sentiment labels
        self.allowed_sentiments = allowed_sentiments or ["Positive", "Negative", "Neutral"]

    def is_valid(self, tweet: dict) -> bool:
        for field in self.required_fields:
            if field not in tweet:
                return False
            if tweet[field] is None:
                return False
            if isinstance(tweet[field], str) and tweet[field].strip() == "":
                return False

        # Type checks
        if not isinstance(tweet.get("Tweet"), str):
            return False
        if not isinstance(tweet.get("name"), str):
            return False
        if not isinstance(tweet.get("prediction"), (int, float)):
            return False

        # Sentiment label check
        if tweet.get("sentiment") not in self.allowed_sentiments:
            return False

        return True

    def filter_valid(self, data: list) -> list:
        """
        Filters a list of tweets and returns only the valid ones.
        """
        return [tweet for tweet in data if self.is_valid(tweet)]

    def report_invalid(self, data: list) -> list:
        """
        Returns a list of invalid tweets for review or logging.
        """
        return [tweet for tweet in data if not self.is_valid(tweet)]