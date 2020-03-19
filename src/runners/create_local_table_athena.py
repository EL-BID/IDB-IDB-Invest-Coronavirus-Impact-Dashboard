from pathlib import Path
home = Path().home()

import sys
sys.path = sys.path + [str(home) + '/.conda/envs/condaenv/lib/python37.zip', 
                       str(home) + '/.conda/envs/condaenv/lib/python3.7', 
                       str(home) + '/.conda/envs/condaenv/lib/python3.7/lib-dynload', 
                       str(home) + '/.conda/envs/condaenv/lib/python3.7/site-packages']
sys.prefix = str(home / '.conda/envs/condaenv/')

from pathlib import Path
import pandas as pd
import logging

from sklearn.cluster import DBSCAN
import shapely.wkt
import networkx as nx
from shapely.geometry import LineString
from collections import defaultdict
import pickle as p

from utils import safe_create_path, to_wkt, query_athena, generate_query, get_geometry, get_data_from_athena

log = logging.getLogger(__name__)

def _save_local(df, config, columns, replace=True):

    path = ( Path.home()
        / 'shared'
        / '/'.join(config['s3_path'].split('/')[3:])
        / config['slug']
        / config['current_millis']
        / config['raw_table']
        / 'temp_tables'
        / config['name'])
            
    safe_create_path(path, replace)

    df[columns]\
        .to_csv(path / (config['name'] + '.csv'),
                index=False, header=False, sep='|')


    

def check_existence(config):

    res = get_data_from_athena(
                f"show tables in {config['athena_database']} '{config['slug']}_{config['name']}'")

    return len(res) > 0

def start(config):

    globals()[config['name']](config)

    sql = (Path(__file__).cwd() / config['path'])

    query = generate_query(sql, config)

    query_athena(query, config)

    