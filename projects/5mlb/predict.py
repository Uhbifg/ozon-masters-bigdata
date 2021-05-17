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
from sklearn.svm import LinearSVC
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import Pipeline, PipelineModel


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        test_path = str(sys.argv[1])
        model_name = sys.argv[2]
        model_ver = int(sys.argv[3])
        out_path = sys.argv[4]
    except:
        logging.critical("Need to pass both project_id and train dataset path")
        sys.exit(1)
    os.system('hdfs dfs -getmerge {} test.pickle'.format(test_path))
    X_test = pd.read_pickle("test.pickle")
    X_test.verified["verified"].map({"true" : 1, "false" : 0})
    X_test.fillna(0)
    X_test = X_test.astype(float)
    spark_df = spark.CreateDataFrame(X_test)
    model = mlflow.pyfunc.spark_udf(spark, "models:/{}/{}".format(model_name, model_ver))
    cols = spark_df.select("vote", "verified").schema.fieldNames()
    spark_df.withColumn("prediction", model(*cols)).select("id", "prediction").repartition(1).write.format("com.databricks.spark.csv").option("header", "false").save(out_path)