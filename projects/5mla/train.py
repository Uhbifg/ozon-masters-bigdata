from sklearn.metrics import accuracy_score, log_loss
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
import os, sys
import logging
import mlflow
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import pandas as pd

from catboost import CatBoostRegressor

if __name__ == "__main__":
    try:
        model_param = int(sys.argv[1])
        train_path = sys.argv[2]
    except:
        logging.critical("Need to pass both project_id and train dataset path")
        sys.exit(1)

    numeric_features = ["if"+str(i) for i in range(1,14)]
    categorical_features = ["cf"+str(i) for i in range(1,27)] + ["day_number"]
    fields = ["id", "label"] + numeric_features + categorical_features 
    model_features = numeric_features + categorical_features 



    read_table_opts = dict(sep="\t", names=fields, index_col=False, nrows=150)
    df = pd.read_table(train_path, **read_table_opts)

    X_train = df[model_features]
    y_train = df.label
    X_train = X_train.fillna("0")
    #df = df.applymap(str)
    with mlflow.start_run():

        model = CatBoostRegressor(random_seed=model_param, iterations=1)
        model.fit(X=X_train, y=y_train, cat_features=categorical_features)
        y_proba = model.predict(X_train)
        loss = log_loss(y_train, y_proba)
        mlflow.log_metric("log_loss", 100)
        mlflow.log_param("model_param1", model_param)
        mlflow.catboost.log_model(model, artifact_path="model")






