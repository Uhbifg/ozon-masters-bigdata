#!/opt/conda/envs/dsenv/bin/python
import os, sys
import logging
import mlflow

if __name__ == "__main__":
    try:
        train_path = str(sys.argv[1])
        logging.critical(train_path)
        model_name = sys.argv[2]
        logging.critical(model_name)
        random_state = int(sys.argv[3])
        logging.critical(random_state)
        run_id = sys.argv[4]
        logging.critical(run_id)
    except:
        logging.critical("Need to pass both project_id and train dataset path 1 ")
        sys.exit(1)
    os.system('hdfs dfs -getmerge {} train.pickle'.format(train_path))
    df = pd.read_pickle("train.pickle")
    with mlflow.start_run(run_uuid=run_id):
        X_train, y_train = df.drop(column["id", "label"]), df["label"]
        X_train.verified["verified"].map({"true" : 1, "false" : 0})
        X_train.fillna(0)
        X_train = X_train.astype(float)
        y_train = y_train.astype(float)
        model = LinearSVC(random_state=random_state, max_iter=2)
        model.train(X_train, y_train)
        mlflow.sklearn.log_model(model, artifact_path=model_name)
        