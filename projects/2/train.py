#!/opt/conda/envs/dsenv/bin/python

import os, sys
import logging


import pandas as pd
from sklearn.model_selection import train_test_split
from joblib import dump

#
# Import model definition
#
from model import model, fields, model_features


#
# Logging initialization
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#
# Read script arguments
#
try:
    proj_id = sys.argv[1] 
    train_path = sys.argv[2]
except:
    logging.critical("Need to pass both project_id and train dataset path")
    sys.exit(1)
logging.info(f"TRAIN_ID {proj_id}")
logging.info(f"TRAIN_PATH {train_path}")

read_table_opts = dict(sep="\t", names=fields, index_col=False)
df = pd.read_table(train_path, **read_table_opts)

X_train = df[model_features]
y_train = df.label

model.fit(X_train, y_train)

dump(model, "2.joblib")
