from src import utils

def check_existence():

    pass

def start(attr):


    utils.query_athena('select * from spd_sdv_waze_preprocessing_production.lima limit 10', 
                   config={'dryrun': False}, 
                   config_path='../../configs/athena.yaml')