# Author: WEE LING HUE

#lemmatize the eng words
from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
from nltk.corpus import wordnet

class lemmatizer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(self, inputCol="words", outputCol="lemmatized_words"):
        super(lemmatizer, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol
        self.lemmatizer_udf = udf(self.nltk_lemmatizer, ArrayType(StringType()))

    @staticmethod
    def get_wordnet_pos(word):
        # Map POS tags to WordNet POS format for lemmatization.
        tag = pos_tag([word])[0][1][0].upper()
        tag_dict = {"J": wordnet.ADJ, "N": wordnet.NOUN, "V": wordnet.VERB, "R": wordnet.ADV}
        return tag_dict.get(tag, wordnet.NOUN)

    @staticmethod
    def nltk_lemmatizer(words):
        # Lemmatize words using WordNetLemmatizer.
        lemmatizer = WordNetLemmatizer()
        return [lemmatizer.lemmatize(word, lemmatizer.get_wordnet_pos(word)) for word in words]

    def _transform(self, df):
        return df.withColumn(self.outputCol, self.lemmatizer_udf(col(self.inputCol)))
