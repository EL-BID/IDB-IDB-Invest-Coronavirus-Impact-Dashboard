from pyathena import connect
from jinja2 import Template
import time
import yaml
from pathlib import Path
import logging
import os
from contextlib import contextmanager
from datetime import datetime
import json
import pandas as pd
from rdp import rdp
import csv
import io
from dateutil import rrule

from contextlib import contextmanager


class CsvFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, delimiter='|')

    def format(self, record):
        self.writer.writerow([record.levelname, *record.msg])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()


def logger():
    log = logging.getLogger(__name__)
    log_path = Path.home() / 'shared' /  'spd-sdv-omitnik-waze'  / 'logs' / 'preprocessing' 
    log_name = datetime.now().strftime('%Y-%m-%d') + '.csv'
    safe_create_path(log_path)

    logging.basicConfig(level=logging.INFO,
        handlers=[
            logging.FileHandler(str(log_path / log_name)),
            logging.StreamHandler()
        ])
    logging.root.handlers[0].setFormatter(CsvFormatter())
    logging.root.handlers[1].setFormatter(CsvFormatter())
    return log


def printlog(name, config, t, time_chunk, force):

    log.info([config['slug'], 
            datetime.strptime(config['current_millis'], '%Y-%m-%d-%H-%M-%S')\
                    .strftime('%Y-%m-%d %H:%M:%S'),
            name, t, time_chunk, force])

@contextmanager
def timed_log(name, config, time_chunk='seconds', force=False):
    """Context manager to get execution time of parts of codes.

    To use, simply declares the context manager:

        `with timed_log(name='useful', time_chunck='minutes'):
            -- your code to get execution time here
        `
    
    Do not forget to import the logging library and set it to print INFO logs.

    Parameters
    ----------
    name : str
        a useful name for you to know which part of the code you are measuring
    time_chunck : str, optional
        can be `minutes` or `seconds`, by default 'seconds'
    """
    
    # printlog = lambda name, t, time_chunk: log.info(' '.join(map(str, [name, 'took', t, time_chunk]))) 
    
    start_time = time.time()
    try:
        yield start_time
    finally:
        
        total_time = time.time() - start_time
        
        if time_chunk == 'seconds':
            printlog(name, config, round(total_time, 1), time_chunk, force)
        elif time_chunk == 'minutes':
            printlog(name, config, round(total_time / 60, 2), time_chunk, force)


def get_config(path='configs/config.yaml'):
    """Load configuration file
    
    Parameters
    ----------
    path : str, optional
        config.yaml path, by default 'configs/config.yaml'
    
    Returns
    -------
    dict
        variables from config.yaml
    """
    
    config = yaml.load(open(path))

    current_time_string = datetime.strftime(
                          datetime.now(),
                          '%Y-%m-%d-%H-%M-%S')
    
    config.update(dict(current_millis=current_time_string))
    
    return config


def get_dependency_graph(path='configs/dependency_graph.yaml'):
    """Load dependency_graph file
    
    Parameters
    ----------
    path : str, optional
        dependency_graph.yaml path, by default 'configs/dependency_graph.yaml'
    
    Returns
    -------
    dict
        variables from dependency_graph.yaml
    """
    
    config = yaml.load(open(path))
    
    return config


def connect_athena(path='configs/athena.yaml'):
    """Gets athena cursor given athena an athena configuration file.`
    
    Returns
    -------
    pyathena cursor
        Athena database cursor
    """

    athena_config = yaml.load(open(path, 'r'))
    
    return connect(**athena_config)


def get_data_from_athena(query, config=dict(), config_path='configs/athena.yaml'):
    
    con = connect_athena(config_path)

    if config.get('verbose'):
        print(query)
    
    res = pd.read_sql_query(query, con)

    return res


def query_athena(query, config, config_path='configs/athena.yaml'):
    """Execute a query in athena
    
    Parameters
    ----------
    query : str
        a valid sql query
    """
    cursor = connect_athena(config_path).cursor()
 
    if config['verbose']:
        print(query)

    if not config['dryrun']:

        if config['force']:

            cursor.execute(f'drop table \
                {config["athena_database"]}.{config["slug"]}_{config["raw_table"]}_{config["name"]}')

        cursor.execute(query)

    cursor.close()

def generate_query(path, config):
    """Fills a string with jinja2 placeholders, {{ }}, with variables from the config.yaml
    file.
    
    Parameters
    ----------
    path : str
        path to file
    config : dict
        config.yaml variables
    
    Returns
    -------
    str
        string with placeholders replaced
    """
    
    sql = open(path, 'r').read()

    query = Template(sql).render(**config)
    
    if config['verbose']:
        print(query)

    return query

    
def to_wkt(x):

    return 'polygon' + str(x).replace('], [', ',')\
                            .replace(', ', ' ')\
                            .replace('[', '(')\
                            .replace(']', ')')


def safe_create_path(path, replace=False):
    
    try:
        if replace:
            if os.path.isfile(path):
                shutil.rmtree(path)
        os.makedirs(path)
    except Exception as e:
        pass

try:
    log = logger()
except IndexError:
    pass


def break_list_in_chunks(data, chunk):
    return [data[x:x+chunk] for x in range(0, len(data), chunk)]


def add_query_dates(start, end):

    if end == 'today':
        end = datetime.now()

    return  [dt.strftime("%Y%m%d") 
                    for dt in rrule.rrule(rrule.DAILY,
                                            dtstart=start, until=end)]