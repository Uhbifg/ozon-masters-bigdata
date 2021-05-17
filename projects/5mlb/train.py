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

if __name__ == "__main__":
    try:
        train_path = str(sys.argv[1])
        model_name = sys.argv[2]
        random_state = int(sys.argv[3])
        run_id = sys.argv[4]
    except:
        logging.critical("Need to pass both project_id and train dataset path")
        sys.exit(1)
    os.system('hdfs dfs -getmerge {} train.pickle'.format(train_path))
    df = pd.read_pickle("train.pickle")
    with mlflow.start_run(run_uuid=run_id):
        X_train, y_train = df.drop(column["id", "label"]), df["label"]
        X_train.verified["verified"].map({"true" : 1, "false" : 0})
        X_train.fillna(0)
        model = LinearSVC(random_state=random_state, max_iter=2)
        model.train(X_train, y_train)
        mlflow.sklearn.log_model(model, artifact_path=model_name)
        