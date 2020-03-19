from pathlib import Path

from src import utils

def _get_dates(dates):

    return '|'.join(
            utils.add_query_dates(dates['start'], dates['end']))

def historical_2020_raw(config):

    config['dates'] = _get_dates(config['historical_2020_interval'])

    return config

def historical_2019_raw(config):

    config['dates'] = _get_dates(config['historical_2019_interval'])

    return config

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