
import logging
from paths import home
import os
from pathlib import Path
import logging
import time
import json
import warnings
import networkx as nx
import pickle as p
import importlib
warnings.filterwarnings('ignore')

from utils import safe_create_path, get_data_from_athena, to_wkt, get_geometry, generate_query, query_athena, get_config, connect_athena, timed_log


def do_drop_tables(config):
    """Drop tables with same preffix defined by {{ slug }}
    
    Parameters
    ----------
    config
    """

    cursor = connect_athena().cursor()

    if config['runall']:
        query = f"show tables in {config['athena_database']} '{config['slug']}*'"
    else:
        query = f"show tables in {config['athena_database']} '{config['slug']}_*'"

    if config['verbose']:
        print(f'Deleting: {query}')
    for table in cursor.execute(query).fetchall():
        if config['verbose']:
            print(table[0])

        cursor.execute(f"drop table {config['athena_database']}.{table[0]}")

    # Extra time to delete tables
    time.sleep(5)
    cursor.close()

def write_report(config, config_path, geometry_path):

    os.environ['CONFIG_PATH'] = str(home) + '/projects/waze_data_normalization/' + config_path
    os.environ['GEOMETRY_PATH'] = str(home) + '/projects/waze_data_normalization/' + geometry_path
    os.environ['MILLIS'] = config['current_millis']

    os.system('jupyter nbconvert --ExecutePreprocessor.timeout=600 --to notebook \
                --execute reports/auto-report.ipynb')

    os.system(f'jupyter nbconvert --to html_toc --no-input \
                --output-dir {config["save_path"]}/support_files/ \
                reports/auto-report.nbconvert.ipynb  ')

    os.system(f'cp reports/auto-report.nbconvert.ipynb \
               {config["save_path"]}/support_files/')

    os.system(f'cp {config_path} \
                {config["save_path"]}/config.yaml')

    os.system(f'cp {geometry_path} \
            {config["save_path"]}/support_files/geometry.json')

    # json.dump(config, 
    # open(
    #     f'~/shared/spd-sdv-omitnik-waze/preprocessed/{config["slug"]}/support_files/config.json',
    #     'w'
    # ))

def save_columns(config):
    
    for table_name in get_data_from_athena(f'show tables in {config["athena_database"]} \'{config["slug"]}*\'')\
                      .iloc[:, 0].tolist():
     
        save_path = config['save_path'] / 'support_files' / 'columns' / table_name / 'columns.json'

        safe_create_path(save_path.parent)

        content = get_data_from_athena(f'show columns in {config["athena_database"]}.{table_name}')

        content = {'columns': [c[0].strip() for c in content.values]}

        json.dump(content, open(save_path, 'w'))

def drop_temp_folder(config):

    os.system('rm -rf ~/shared/spd-sdv-omitnik-waze/preprocessed/{config["slug"]}/temp_tables')

def document(args, config):

    config['save_path'] = Path('/'.join([str(Path.home()), 'shared'] + 
                                        config['s3_path'].split('/')[3:] + 
                                        [config['slug'], config['current_millis']]))

    if not args.dryrun:   
        with timed_log(name='columns', config=config, time_chunk='seconds', force=True):
            save_columns(config)

        if args.report:

            with timed_log(name='report', config=config, time_chunk='seconds', force=args.report):
                write_report(config, args.config_path, args.geometry_path)
        
        if args.flush_temp_tables:

            for table in tables_to_drop:
                query_athena(
                    f'drop table {config["athena_database"]}.{config["slug"]}_{table}',
                    config)    

            drop_temp_folder(config)