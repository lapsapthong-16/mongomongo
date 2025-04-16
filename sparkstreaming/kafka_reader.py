#------------------------
# Author: Wong Chyi Keat
#------------------------
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

# Class to handle reading and parsing tweet data from Kafka
class KafkaTweetReader:
    def __init__(self, spark):
        # Initialize with a Spark session
        self.spark = spark
        
        # Define the schema for the incoming JSON data from Kafka
        self.schema = StructType() \
            .add("user_id", StringType()) \
            .add("name", StringType()) \
            .add("followers_count", StringType()) \
            .add("tweet_text", StringType()) \
            .add("Location", StringType()) \
            .add("Time", StringType()) \
            .add("Friends Count", StringType()) \
            .add("prediction", StringType()) \
            .add("sentiment", StringType())

    # Function to read and parse the stream from Kafka
    def read_and_parse(self):
        # Read the raw stream from Kafka
        raw = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "BusinessNewsTopic,EntertainmentNewsTopic,MalaysiaNewsTopic,PoliticsNewsTopic,SportsNewsTopic") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

        # Parse the JSON strings from the Kafka messages
        parsed = raw.selectExpr("CAST(value AS STRING) as json_string", "topic") \
            .select(from_json(col("json_string"), self.schema).alias("data"), col("topic")) \
            .select("data.*", "topic") \
            .withColumnRenamed("tweet_text", "Tweet") \
            .filter(col("Tweet").isNotNull()) \
            .filter(col("Time").isNotNull())

        return parsed
