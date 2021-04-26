from pyspark.ml.feature import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline


tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")


stop_words = StopWordsRemover.loadDefaultStopWords("english")
swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="words_filtered", stopWords=stop_words)

hasher = HashingTF(numFeatures=1000, binary=True, inputCol=swr.getOutputCol(), outputCol="word_vector")

lr = LogisticRegression(featuresCol=hasher.getOutputCol(), labelCol="overall", maxIter=100)

pipeline = Pipeline(stages=[
    tokenizer,
    swr,
    hasher,
    lr
])