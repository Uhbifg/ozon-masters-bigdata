import os, sys
import logging
import mlflow

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
    with mlflow.start_run():
        run = mlflow.active_run()
        
        logging.critical(run.info.run_id)
        os.system("/opt/conda/envs/dsenv/bin/python3 etl.py {} {} {}".format(train_path_in, "train", 1))
        os.system("/opt/conda/envs/dsenv/bin/python train.py {} {} {} {}".format("train", model_name, model_param, run.info.run_id))
      