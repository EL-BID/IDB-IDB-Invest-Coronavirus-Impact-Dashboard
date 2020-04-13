from multiprocessing.pool import Pool
from functools import partial
from h3 import h3
from pathlib import Path
from copy import deepcopy
import pyathena
import pandas as pd


import logging
log = logging.getLogger(__name__)

from utils import break_list_in_chunks, add_query_dates, to_wkt, query_athena, generate_query, get_data_from_athena

def _load_cities(
    path= 'data/raw/cities_metadata.csv'):

    return pd.read_csv(path)


def _region_slug_partition(config):

    data = get_data_from_athena(
             "select * from "
            f"{config['athena_database']}.{config['slug']}_metadata_metadata_ready ",
            config
            )

    if config.get('if_exists') == 'append':

        # check if table exists

        skip = get_data_from_athena(
            "select distinct region_shapefile_wkt from "
            f"{config['athena_database']}.{config['slug']}_{config['raw_table']}_{config['name']}",
            config
        )

        data = data[~data['region_shapefile_wkt'].isin(skip['region_shapefile_wkt'])]

    if config.get('filter_by_coef'):

        skip = get_data_from_athena(
            "select region_slug from "
            f"{config['athena_database']}.{config['slug']}_analysis_metadata_variation "
            "where weekly_approved = true or daily_approved = true", config
        )

        data = data[data['region_slug'].isin(skip['region_slug'])]       

    if config.get('sample_cities'):

        data = data[:config['sample_cities']]

    data = data.to_dict('records')
    
    for d in data:
        d['partition'] = d['region_slug']
        
    return data


def historical_2019(config):

    return _region_slug_partition(config)


def historical_2020(config):

    return _region_slug_partition(config)


def daily(config):

    return _region_slug_partition(config)

def analysis_daily(config):

    return _region_slug_partition(config)

def perform_query(query):
    """Simply calls Athena and logs the exceptions.
    
    Parameters
    ----------
    query : dict
        dict with two objects, `make` and `drop`. The first to create
        a table and the second to drop the same table.
    """
    print(query['partition'])
    for i in range(query['config']['n_tries']):
        try:
            
            query_athena(query['drop'], query['config'])

            query_athena(query['make'], query['config'])

            query_athena(query['drop'], query['config'])
            break
        except Exception as e:
            if query['config']['verbose']:
                print(e)
            continue


def check_existence(config):

    res = get_data_from_athena(
                f"show tables in \
                    {config['athena_database']} '{config['slug']}_{config['raw_table']}_{config['name']}'")

    return len(res) > 0


def partition_query(query_path, config):
    """Entrypoint function.

    In order to run paralel queries to populate the same table, the trick
    is to create an empty table that points to a specific S3 path. Then,
    a bunch of temporary tables are through an Athena process that points
    to the same path. This populates the initial table that now can be
    queried normally by Athena.

    This also allows us to do that asynchronously. This function implements
    that by using `multiprocessing.Pool` from python standard library.

    First, it creates a list of dictionaries, `queries`, with two objects:
        - `make`: a query that creates the table
        - `drop`: a query that drops the same table
    
    Then, this list is called by a Pool that has a number of jobs set by
    the user in the config.yaml file, {{ number_of_athena_jobs }}.

    Parameters
    ----------
    query_path : str    
        path to the query that creates the table
    config : dict
        variables from config.yaml
    """

    queries = []

    partitions = globals()[config['name']](config)

    for partition in partitions:

        config.update(deepcopy(partition))

        queries.append(
            dict(make=generate_query(query_path, config),
                drop=
                f"drop table {config['athena_database']}.{config['slug']}_{config['raw_table']}_{config['name']}_{config['partition']}",
                config=config,
                partition=config['partition']
        ))

    with Pool(config['number_of_athena_jobs']) as p:
        p.map(partial(perform_query), queries)

def start(config):

    sql_path = (Path(__file__).cwd() / config['path'])

    # create table
    sql = generate_query(sql_path / 'create_table.sql', config)

    if not((config.get('if_exists') == 'append') and check_existence(config)):
        print('replacing')
        query_athena(sql, config)

    config['force'] = False

    # fill table  
    partition_query(sql_path / 'fill_table.sql', config)

    # partition
    sql = generate_query(sql_path / 'partition.sql', config)

    if config['verbose']:
        print(sql)

    query_athena(sql, config)