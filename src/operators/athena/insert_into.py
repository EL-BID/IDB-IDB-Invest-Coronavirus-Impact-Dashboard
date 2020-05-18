from multiprocessing.pool import Pool
from functools import partial
from pathlib import Path
from copy import deepcopy
import pyathena
import pandas as pd

import logging

log = logging.getLogger(__name__)

from utils import (
    query_athena,
    generate_query,
)


def perform_query(query):
    """Simply calls Athena and logs the exceptions.
    
    Parameters
    ----------
    query : dict
        dict with two objects, `make` and `drop`. The first to create
        a table and the second to drop the same table.
    """

    for i in range(query["config"]["n_tries"]):
        try:

            query_athena(query["make"], query["config"])

            break

        except Exception as e:
            if query["config"]["verbose"]:
                print(e)
            if i == query["config"]["n_tries"] - 1:
                raise e
            continue


def insert_into(query_path, config, insert_groups):

    config["force"] = False
    
    if not isinstance(insert_groups, list):
        insert_groups = globals()[config["name"]](config)
    
    queries = []
    for insert_group in insert_groups:

        config.update(deepcopy(insert_group))

        queries.append(
            dict(
                make=generate_query(query_path, config),
                config=deepcopy(config)
            ))

    with Pool(config["number_of_athena_jobs"]) as p:
        p.map(partial(perform_query), queries)

def check_existence(config):

    res = get_data_from_athena(
        f"show tables in \
                    {config['athena_database']} '{config['slug']}_{config['raw_table']}_{config['name']}'"
    )

    return len(res) > 0

def should_create_table(config):

    try:
        current_millis = get_data_from_athena(
            f"""
                select split("$path", '/')[7] current_millis 
                from {config['athena_database']}.{config['slug']}_{config['raw_table']}_{config['name']} 
                limit 1""",
        )
    except:
        current_millis = []

    if len(current_millis):
        # Check if current_millis is the same as the one saved
        return current_millis["current_millis"][0] != config.get("current_millis")
    else:
        return True


def start(config, insert_groups=None):

    sql_path = Path(__file__).cwd() / config["path"]

    # create table
    if should_create_table(config):
        query_athena(generate_query(sql_path / "create_table.sql", config), config)

    # fill table
    insert_into(sql_path / "insert_into.sql", config, insert_groups)

