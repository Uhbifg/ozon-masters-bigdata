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
        test_path_in = str(sys.argv[1])
        model_name = sys.argv[2]
        model_param = sys.argv[3]
    except:
        logging.critical("Need to pass both project_id and train dataset path")
        sys.exit(1)
    with mlflow.start_run() as run:
        os.system("etl.py {} {} {}".format(train_path_in, "train", 1))
        os.system("train.py {} {} {} {}".format("train", model_name, model_param, run.info.run_id))
      