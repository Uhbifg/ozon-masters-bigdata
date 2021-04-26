#!/opt/conda/envs/dsenv/bin/python
import os, sys
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline, PipelineModel


model_path = sys.argv[1]
test_path = sys.argv[2]
y_path = sys.argv[3]

schema = StructType([
    StructField("overall", FloatType()),
    StructField("vote", StringType()),
    StructField("verified", StringType()),
    StructField("reviewTime", StringType()),
    StructField("reviewerID", StringType()),
    StructField("asin", StringType()),
    StructField("reviewerName", StringType()),
    StructField("reviewText", StringType()),
    StructField("summary", StringType()),
    StructField("unixReviewTime", StringType())
])


model = PipelineModel.load(model_path)
test = spark.read.json(test_path, schema=schema)
y = model.transform(test)
predictions.select("id", "prediction").write.save(y_path)
spark.stop()
