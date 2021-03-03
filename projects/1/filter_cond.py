logging.basicConfig(level=logging.DEBUG)
logging.info("CURRENT_DIR {}".format(os.getcwd()))
logging.info("SCRIPT CALLED AS {}".format(sys.argv[0]))
logging.info("ARGS {}".format(sys.argv[1:]))

import sys

def filter_cond(line_dict):
    """Filter function
    Takes a dict with field names as argument
    Returns True if conditions are satisfied
    """
    if(line_dict["if1"] == ''):
        return False
    cond_match = (
       (int(line_dict["if1"]) > 20 and int(line_dict["if1"]) < 40)
    ) 
    return True if cond_match else False
