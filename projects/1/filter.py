#!/opt/conda/envs/dsenv/bin/python

import sys
import os
from glob import glob
import logging

sys.path.append('.')
from model import fields, train_features

#
# Init the logger
#
logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

#
# import the filter
#
filter_cond_files = glob('filter_cond*.py')
logging.info(f"FILTERS {filter_cond_files}")


if len(filter_cond_files) != 1:
    logging.critical("Must supply exactly one filter")
    sys.exit(1)

exec(open(filter_cond_files[0]).read())


for line in sys.stdin:
    # skip header
    if line.rstrip().startswith(fields[0]):
        continue

    #unpack into a tuple/dict
    values = line.rstrip().split('\t')
    test = dict(zip(train_features, values)) 
    
    #apply filter conditions
    if filter_cond(test):
        test = "\t".join([test[x] for x in train_features])
        #logging.info(test)
        sys.exit(1)
        print(test)
