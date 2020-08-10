from pathlib import Path

from src import utils


def check_existence(config):

    res = utils.get_data_from_athena(
                f"show tables in {config['athena_database']} '{config['slug']}_{config['raw_table']}_{config['name']}'")

    return len(res) > 0


def start(config):

    if config['name'] in globals().keys():

        config = globals()[config['name']](config)

    sql = (Path(__file__).cwd() / config['path'])

    query = utils.generate_query(sql, config)

    utils.query_athena(query, config)