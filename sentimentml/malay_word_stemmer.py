# Author: WEE LING HUE

# stemming the malay words
from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory


class malay_word_stemmer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(self, inputCol="lemmatized_words", outputCol="stemmed_words"):
        super(malay_word_stemmer, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, df):
        stemmer_udf = udf(self.malay_stemmer, ArrayType(StringType()))
        return df.withColumn(self.outputCol, stemmer_udf(col(self.inputCol)))

    @staticmethod
    def malay_stemmer(words):
        # Stem Malay words using Sastrawi.
        factory = StemmerFactory()
        stemmer = factory.create_stemmer()
        return [stemmer.stem(word) for word in words]
