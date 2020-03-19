from multiprocessing.pool import Pool
from functools import partial
from h3 import h3
from pathlib import Path
from copy import deepcopy
import pyathena
import pandas as pd


import logging
log = logging.getLogger(__name__)

from utils import break_list_in_chunks, add_query_dates, to_wkt, query_athena, generate_query, get_geometry, get_data_from_athena

def _load_cities(
    path= 'data/raw/cities_metadata.csv'):

    return pd.read_csv(path)

def _create_cities_partitions(config):
    
    cities = _load_cities()
    
    c = cities.query(f'city_slug in {config["cities"]}')\
          .query(f'shape_type == "{config["shape_type"]}"')
    
    return c[['country_name', 
              'country_iso',
              'city_name',
              'city_slug',
              'timezone',
              'city_shapefile_wkt']].rename(
                  columns={'city_slug': 'partition'}).to_dict('records')


def _country_partition(config):

    return [{'partition': country} 
            for country in set(config['countries'])]


def historical_2019(config):

    return _country_partition(config)

def historical_2020(config):

    return _country_partition(config)

def perform_query(query):
    """Simply calls Athena and logs the exceptions.
    
    Parameters
    ----------
    query : dict
        dict with two objects, `make` and `drop`. The first to create
        a table and the second to drop the same table.
    """
    for i in range(query['config']['n_tries']):
        try:
            print(query['drop'])
            query_athena(query['drop'], query['config'])

            query_athena(query['make'], query['config'])

            query_athena(query['drop'], query['config'])
            break
        except Exception as e:
            print(e)

def check_existence(config):

    res = get_data_from_athena(
                f"show tables in \
                    {config['athena_database']} '{config['slug']}_{config['name']}'")

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

    if config['verbose']:
        print(sql)

    query_athena(sql, config)

    config['force'] = False

    # fill table  
    partition_query(sql_path / 'fill_table.sql', config)

    # partition
    sql = generate_query(sql_path / 'partition.sql', config)

    if config['verbose']:
        print(sql)

    query_athena(sql, config)