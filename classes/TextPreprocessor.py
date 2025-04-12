# Author: WEE LING HUE

from classes.TextCleaner import TextCleaner  # Import TextCleaner
from classes.Lemmatizer import Lemmatizer  # Import Lemmatizer
from classes.MalayStemmer import MalayStemmer  # Import MalayStemmer
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, StringType

class TextPreprocessor:
    def __init__(self, input_col="Tweet", label_col="Sentiment"):
        self.input_col = input_col
        self.label_col = label_col

        # Preprocessing steps
        self.text_cleaner = TextCleaner(inputCol=self.input_col, outputCol="clean_text")
        self.tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
        self.stopword_remover_eng = StopWordsRemover(inputCol="words", outputCol="filtered_words_eng")
        self.stopword_remover_malay = StopWordsRemover(
            inputCol="filtered_words_eng", outputCol="filtered_words", stopWords=self.get_malay_stopwords()
        )
        self.lemmatizer = Lemmatizer(inputCol="filtered_words", outputCol="lemmatized_words")
        self.stemmer = MalayStemmer(inputCol="lemmatized_words", outputCol="stemmed_words")
        self.hashing_tf = HashingTF(inputCol="stemmed_words", outputCol="raw_features", numFeatures=10000)
        self.idf = IDF(inputCol="raw_features", outputCol="features")
        self.label_indexer = StringIndexer(inputCol=self.label_col, outputCol="label")

    def get_pipeline(self):
        # Returns a PySpark Pipeline for text preprocessing.
        return Pipeline(stages=[
            self.text_cleaner,           # Clean text (remove URLs, mentions, special characters)
            self.tokenizer,              # Tokenize words
            self.stopword_remover_eng,   # Remove English stopwords
            self.stopword_remover_malay, # Remove Malay stopwords
            self.lemmatizer,             # Lemmatize words
            self.stemmer,                # Apply Malay stemming
            self.hashing_tf,             # Convert text to TF features
            self.idf,                    # Compute IDF scores
            self.label_indexer           # Encode labels
        ])

    @staticmethod
    def get_malay_stopwords():
        # Return a list of common Malay stopwords.
        return ["saya", "awak", "dia", "kita", "kami", "mereka", "ini", "itu", "dalam", "dan", "di", 
                "ke", "dari", "adalah", "yang", "untuk", "dengan", "atau", "seperti", "tetapi", "kerana", "oleh", "pada"]

