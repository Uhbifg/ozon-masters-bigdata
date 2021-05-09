from catboost import CatBoostRegressor
from sklearn.metrics import accuracy_score, log_loss
from sklearn import preprocessing
from sklearn.linear_model import LogisticRegression
import os, sys
import logging


import pandas as pd

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
    model_features = numeric_features + categorical_features + ["day_number"]
    train_features = ["id"] + numeric_features + ["cf"+str(i) for i in range(1,27)]


    mlflow.sklearn.autolog()

    read_table_opts = dict(sep="\t", names=fields, index_col=False, nrows=150)
    df = pd.read_table(train_path, **read_table_opts)

    X_train = df[model_features]
    y_train = df.label


    with mlflow.start_run():
        preprocessor = ColumnTransformer(
        transformers=[
            ('cat', preprocessing.LabelEncoder(), categorical_features)
        ]
    )

        model = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('linearregression', LogisticRegression(random_state=model_param, max_iter=100))
        ])
        model.fit(X_train, y_train)
        y_proba = model.predict(X_train)
        loss = log_loss(y_train, y_proba)
        mlflow.log_metric("log_loss", 100)
        mlflow.log_param("model_param1", model_param1)







