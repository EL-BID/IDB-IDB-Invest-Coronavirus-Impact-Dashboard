from pathlib import Path
home = Path().home()

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import logging
from df2gspread import df2gspread as d2g

from utils import safe_create_path, get_data_from_athena, generate_query, query_athena

log = logging.getLogger(__name__)


def _save_local(df, config, columns, replace=True):

    path = ( Path.home()
        / 'shared'
        / '/'.join(config['s3_path'].split('/')[3:])
        / config['slug']
        / config['current_millis']
        / config['raw_table']
        / config['name'])
            
    safe_create_path(path, replace)

    df[columns]\
        .to_csv(path / (config['name'] + '.csv'),
                index=False, header=False, sep='|')

def _get_credentials():
    
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
             'configs/gcloud-credentials.json', scope) 

    gc = gspread.authorize(credentials) 

    return credentials, gc


def _write_sheets_table(df, config, drive_config):

    credentials, gc = _get_credentials()

    spreadsheet_key = drive_config['spreadsheet_key']
    wks_name = drive_config['worksheet']
    d2g.upload(df,
               spreadsheet_key, 
               wks_name, 
               row_names=False,
               credentials=credentials)

def _read_sheets_tables():
    
    credentials, gc = _get_credentials()

    gc = gspread.authorize(credentials)
    
    wks = gc.open('regions_metadata')

    datasets = {}
    for w in wks.worksheets():

        data = w.get_all_values()

        headers = data.pop(0)

        datasets[w.title] = pd.DataFrame(data, columns=headers)

    return datasets


def _add_table(config):

    sql = (Path(__file__).cwd() / config['path'])

    query = generate_query(sql, config)

    query_athena(query, config)


def load_metadata_tables(config):

    dfs = _read_sheets_tables()

    for name in dfs.keys():

        if name in config['metadata'].keys():

            config['name'] = name

            config['columns'] = config['metadata'][name]['columns']

            _add_table(config)

            _save_local(dfs[name], 
                        config,
                        config['columns']
                        )

def write_index(config):

    df = get_data_from_athena(
            "select * from "
            f"{config['athena_database']}.{config['slug']}_"
            f"{config['raw_table']}_index"
        )


    _write_sheets_table(df, config, config['drive_config'][config['name']][config['slug']])
    
def check_existence(config):

    res = get_data_from_athena(
                f"show tables in {config['athena_database']} '{config['slug']}_{config['raw_table']}_{config['name']}'")

    return len(res) > 0

def start(config):

    globals()[config['name']](config)

    # sql = (Path(__file__).cwd() / config['path'])

    # query = generate_query(sql, config)

    # query_athena(query, config)

    