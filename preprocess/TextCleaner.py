# Author: WEE LING HUE

from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, ArrayType
import re

class TextCleaner(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(self, inputCol="Tweet", outputCol="clean_text"):
        super(TextCleaner, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, df):
        clean_text_udf = udf(self.cleaning_text, StringType())
        return df.withColumn(self.outputCol, clean_text_udf(col(self.inputCol)))

    @staticmethod
    def cleaning_text(text):
        # text cleaning: remove URLs, mentions, special characters, and lowercasing.
        if text:
            text = text.lower()
            text = re.sub(r"http\S+", "", text)  # Remove URLs
            text = re.sub(r"@\w+", "", text)  # Remove @mentions
            text = re.sub(r"[^a-zA-Z0-9\s]", "", text)  # Remove special characters
            text = re.sub(r"\s+", " ", text).strip()  # Remove extra spaces
        return text
