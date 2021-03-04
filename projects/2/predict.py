#!/opt/conda/envs/dsenv/bin/python

import sys, os
import logging
from joblib import load
import pandas as pd

sys.path.append('.')
from model import fields, train_features, model_features

#
# Init the logger
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#load the model
model = load("2.joblib")

read_opts=dict(
        sep='\t', names=train_features, index_col=False, header=None,
        iterator=True, chunksize=100
)


for df in pd.read_csv(sys.stdin, **read_opts):
    df.replace({'\N': None}, inplace=True)
    X_test = df[model_features].astype('Int64', copy=False)
    pred = model.predict(X_test)
    out = zip(df.id, pred)
    print("\n".join(["{0}\t{1}".format(*i) for i in out]))