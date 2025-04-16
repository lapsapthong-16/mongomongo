#------------------------
# Author: Wong Chyi Keat
#------------------------
from pyspark.sql.functions import col
from pyspark.sql.utils import StreamingQueryException
from preprocess.TextPreprocessor import TextPreprocessor  # Import custom text preprocessing class from Task 2
import logging

# Set up logger for tracking and debugging
logger = logging.getLogger("PipelineTrainer")

# Class responsible for training a Spark ML pipeline using static CSV data
class PipelineTrainer:
    def __init__(self, spark):
        # Initialize with a Spark session
        self.spark = spark

    # Method to train the ML pipeline on historical tweet sentiment data
    def train_pipeline(self, csv_path="tweets_output_with_sentiment.csv"):
        try:
            # Read CSV file into a DataFrame, Remove records with null tweets and Remove empty string tweets
            static_df = self.spark.read.csv(csv_path, header=True, inferSchema=True) \
                .selectExpr("Tweet", "Sentiment as sentiment") \
                .filter(col("Tweet").isNotNull()) \
                .filter(col("Tweet") != "")

            # Initialize the text preprocessor with specified input and label columns
            text_preprocessor = TextPreprocessor(input_col="Tweet", label_col="sentiment")

            # Build the full Spark ML pipeline (e.g., tokenizer, TF-IDF, label indexer, classifier)
            pipeline = text_preprocessor.get_pipeline()

            # Fit the pipeline to the static DataFrame to train the model
            model = pipeline.fit(static_df)

            return model

        # Handle exceptions specific to streaming (though CSV read is static, this ensures consistency)
        except StreamingQueryException as e:
            logger.error("Streaming Query Exception: %s", e)

        except Exception as e:
            logger.error("General Exception: %s", e)

