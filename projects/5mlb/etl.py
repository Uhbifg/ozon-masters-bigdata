#!/opt/conda/envs/dsenv/bin/python

from sklearn.metrics import accuracy_score, log_loss
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
import os, sys
import logging
import mlflow
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import Pipeline, PipelineModel


if __name__ == "__main__":
    try:
        train_path = str(sys.argv[1])
        out_path = sys.argv[2]
        is_train = int(sys.argv[3])
    except:
        logging.critical("Need to pass both project_id and train dataset path")
        sys.exit(1)
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    if is_train == 0:
        schema = StructType([
        StructField("id", StringType()),
        StructField("vote", StringType()),
        StructField("verified", StringType()),
        StructField("reviewTime", StringType()),
        StructField("reviewerID", StringType()),
        StructField("asin", StringType()),
        StructField("reviewerName", StringType()),
        StructField("reviewText", StringType()),
        StructField("summary", StringType()),
        StructField("unixReviewTime", StringType())])
        df = spark.read.json(train_path, schema=schema)
        df.select("id", "vote", "verified").to_pandas().to_pickle(out_path)
    else:
        schema = StructType([
        StructField("id", StringType()),
        StructField("label", FloatType()),
        StructField("vote", StringType()),
        StructField("verified", StringType()),
        StructField("reviewTime", StringType()),
        StructField("reviewerID", StringType()),
        StructField("asin", StringType()),
        StructField("reviewerName", StringType()),
        StructField("reviewText", StringType()),
        StructField("summary", StringType()),
        StructField("unixReviewTime", StringType())])
        df.select("id","label", "vote", "verified").to_pandas().to_pickle(out_path)
    spark.stop()
