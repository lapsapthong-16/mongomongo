# Author: Edwina Hon Kai Xin

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
                print(f"Missing field: {field}")
                return False
            if tweet[field] is None:
                print(f"Field is None: {field}")
                return False
            if isinstance(tweet[field], str) and tweet[field].strip() == "":
                print(f"Field is empty string: {field}")
                return False

        # Type checks
        if not isinstance(tweet.get("Tweet"), str):
            print(f"'Tweet' is not a string: {tweet.get('Tweet')}")
            return False
        if not isinstance(tweet.get("name"), str):
            print(f"'name' is not a string: {tweet.get('name')}")
            return False
        if not isinstance(tweet.get("prediction"), str):
            print(f"'prediction' is not a number: {tweet.get('prediction')}")
            return False

        # Sentiment label check
        if tweet.get("sentiment") not in self.allowed_sentiments:
            print(f"Invalid sentiment label: {tweet.get('sentiment')}")
            return False

        return True

    def filter_valid(self, data: list) -> list:
        return [tweet for tweet in data if self.is_valid(tweet)]

    def report_invalid(self, data: list) -> list:
        return [tweet for tweet in data if not self.is_valid(tweet)]