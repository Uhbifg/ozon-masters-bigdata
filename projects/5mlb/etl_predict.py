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
    try:
        test_path_in = str(sys.argv[1])
        predict_path_out  = sys.argv[2]
        sklearn_model  = sys.argv[3]
        model_version = sys.argv[4]
    except:
        logging.critical("Need to pass both project_id and train dataset path")
        sys.exit(1)
    with mlflow.start_run():
        run = mlflow.active_run()
        os.system("/opt/conda/envs/dsenv/bin/python etl.py {} {} {}".format(train_path_in, "test", 0))
        os.system("/opt/conda/envs/dsenv/bin/python predict.py {} {} {} {}".format("test", sklearn_model, model_version, predict_path_out))