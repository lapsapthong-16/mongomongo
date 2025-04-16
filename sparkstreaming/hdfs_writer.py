#------------------------
# Author: Wong Chyi Keat
#------------------------

# Class to handle writing a DataFrame to HDFS in JSON format
class HDFSWriter:
    def __init__(self, dataframe):
        # Initialize with the parsed DataFrame
        self.df = dataframe

    # Method to write the DataFrame to HDFS as JSON
    def write_json(self):
        # Select and cast necessary fields to string for compatibility
        clean_df = self.df.selectExpr(
            "cast(topic as string) as topic",                      # Kafka topic
            "cast(name as string) as name",                        # User's name
            "cast(user_id as string) as user_id",                  # User ID
            "cast(followers_count as string) as followers_count",  # Follower count
            "cast(Tweet as string) as Tweet",                      # Tweet content
            "cast(Location as string) as Location",                # Tweet location
            "cast(Time as string) as Time",                        # Tweet timestamp
            "cast(prediction as string) as prediction",            # Prediction result
            "cast(sentiment as string) as sentiment"               # Sentiment label
        )

        # Write the cleaned DataFrame to HDFS in JSON format
        query = clean_df.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", "hdfs://localhost:9000/user/student/sentiments_data_json_1") \
            .option("checkpointLocation", "hdfs://localhost:9000/user/student/checkpoints/sentiments_data_json_new_1") \
            .trigger(once=True) \
            .start()

        # Block until the stream processing is complete
        query.awaitTermination()
