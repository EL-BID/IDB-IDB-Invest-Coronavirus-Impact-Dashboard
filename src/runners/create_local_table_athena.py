from pathlib import Path

home = Path().home()

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import logging
from df2gspread import df2gspread as d2g
import yaml
from datetime import timedelta, date
import osm_road_length
from shapely import wkt
import awswrangler as wr
import boto3
from timezonefinder import TimezoneFinder
import os

tf = TimezoneFinder()
from src.utils import (
    simplify,
    safe_create_path,
    get_data_from_athena,
    generate_query,
    query_athena,
)

log = logging.getLogger(__name__)


def _save_local(df, config, columns=None, replace=True, wrangler=False):

    if wrangler:

        res = wr.s3.to_parquet(
            df=df,
            path="s3://{bucket}/{prefix}/{slug}/{raw_table}/{name}".format(**config),
            dataset=True,
            database=config["athena_database"],
            table="{slug}_{raw_table}_{name}".format(**config),
            mode=config["mode"],
            partition_cols=["region_slug"],
            boto3_session=boto3.Session(region_name="us-east-1"),
        )

    else:
        path = (
            Path.home()
            / "shared"
            / "/".join(config["s3_path"].split("/")[3:])
            / config["slug"]
            / config["current_millis"]
            / config["raw_table"]
            / config["name"]
        )


        safe_create_path(path, replace)

        df[columns].to_csv(
            path / (config["name"] + ".csv"), index=False, header=False, sep="|"
        )


def _get_credentials():

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "configs/gcloud-credentials.json", scope
    )

    gc = gspread.authorize(credentials)

    return credentials, gc


def _write_sheets_table(df, freq, config, drive_config):

    credentials, gc = _get_credentials()

    spreadsheet_key = drive_config["spreadsheet_key"]
    wks_name = freq
    d2g.upload(df, spreadsheet_key, wks_name, row_names=False, credentials=credentials)

    
def _write_csv_table(df, freq, config, public=False):

    cm = 'private'
    if public == True:
        cm = 'public'
           
    path = (
            Path.home()
            / "shared"
            / "/".join(config["s3_path"].split("/")[3:])
            / config["slug"]
            /  cm
            / config["raw_table"]
            / config["name"]
        )

    safe_create_path(path)
    
    df.to_csv(
        path / ( freq + ".csv"), 
        index=False, header=True#, sep="|"
    )

def _read_sheets_tables():

    credentials, gc = _get_credentials()

    gc = gspread.authorize(credentials)

    wks = gc.open("regions_metadata")

    datasets = {}
    for w in wks.worksheets():

        data = w.get_all_values()

        headers = data.pop(0)

        datasets[w.title] = pd.DataFrame(data, columns=headers)

    return datasets


def _get_length(x, config):

    lengths = osm_road_length.get(wkt.loads(x))
    lengths = lengths[lengths.index.isin(config["accepted_osm_keys"])]
    return lengths["length"].sum()


def metadata_osm_length(config):

    # Fetch regions configuration
    metadata = get_data_from_athena(
        "select region_slug, region_shapefile_wkt, rerun from "
        f"{config['athena_database']}.{config['slug']}_metadata_metadata_prepare "
        "order by region_slug",
        config,
    )

    # Force rerun
    rerun = metadata[metadata["rerun"] == "TRUE"]

    # Select regions to be update if that is the case, else update all
    if config.get("selected_regions"):

        metadata = metadata[
            metadata["region_slug"].isin(config.get("selected_regions"))
        ]

    # Get current state of table
    try:
        current = get_data_from_athena(
            "select region_slug, region_shapefile_wkt, osm_length from "
            f"{config['athena_database']}.{config['slug']}_metadata_metadata_osm_length "
            "order by region_slug",
            config,
        )
    except:
        current = pd.DataFrame(
            [], columns=["region_slug", "region_shapefile_wkt", "osm_length"]
        )

    # Update just regions that changed their shapes or do not exist yet
    if config["mode"] == "overwrite_partitions":

        try:
            skip = current[current["osm_length"] != ""][["region_shapefile_wkt"]]
        except:
            skip = pd.DataFrame([], columns=["region_shapefile_wkt"])

        selected = metadata[
            ~metadata["region_shapefile_wkt"].isin(skip["region_shapefile_wkt"])
        ][["region_slug", "region_shapefile_wkt"]]

    selected = pd.concat([selected, rerun]).drop_duplicates()

    # Calculate OSM length for selected regions
    selected["osm_length"] = selected["region_shapefile_wkt"].apply(
        lambda x: _get_length(x, config)
    )
    selected = selected.sort_values(by="region_slug")

    # If current table is empty, initilize
    if len(current):
        current.update(selected[["region_slug", "region_shapefile_wkt", "osm_length"]])
    else:
        current = selected[["region_slug", "region_shapefile_wkt", "osm_length"]]

    if config["verbose"]:
        print(current)
        print(list(current["region_slug"]))

    if len(metadata):

        _save_local(current, config, wrangler=True)


def _get_timezone(x, config):

    coords = dict(
        zip(["lng", "lat"], [float(c) for c in x.split(",")[2].strip().split(" ")])
    )

    timezone = TimezoneFinder().timezone_at(**coords)

    return config["timezone"]["replace"].get(timezone, timezone)


def metadata_prepare(config):

    metadata = get_data_from_athena(
        "select * from "
        f"{config['athena_database']}.{config['slug']}_metadata_regions_metadata ",
        config,
    )

    metadata["timezone"] = metadata["region_shapefile_wkt"].apply(
        lambda x: _get_timezone(x, config)
    )

    _save_local(metadata, config, wrangler=True)


def _add_table(config):

    sql = Path(__file__).cwd() / config["path"]

    query = generate_query(sql, config)

    query_athena(query, config)


def load_metadata_tables(config):

    dfs = _read_sheets_tables()

    for name in dfs.keys():

        if name in config["metadata"].keys():

            config["name"] = name

            config["columns"] = config["metadata"][name]["columns"]

            _add_table(config)

            _save_local(dfs[name], config, config["columns"])


def _remove_countries(df, public = False):
    
    # Remove countries columns
    df.loc[df.region_slug.isin(['country_brazil', 'country_mexico']), 'tcp'] = None
    
    df.loc[df.region_slug.isin(['country_brazil', 'country_mexico']), 'dashboard'] = 'FALSE'
    
    print(sum(df.dashboard == 'TRUE'))    
        
    return df
    
    
def write_index(config):

    for table in config["to_write"]:

        df = get_data_from_athena(
            "select * from "
            f"{config['athena_database']}.{config['slug']}_{table['table']}"
        )
        
        # Remove countries observations
        df = _remove_countries(df) 

        # Remove columns overall drop
        if "region_shapefile_wkt" in df.columns:
            df["region_shapefile_wkt"] = df["region_shapefile_wkt"].apply(
                lambda x: str(simplify(wkt.loads(x)))
            )

        if table.get("overall_drop"):
            df = df.drop(table["overall_drop"], 1)

        # print(df.apply(lambda x: max(len(str))))

        drive_config = yaml.load(open("configs/drive-config.yaml", "r"), Loader=yaml.Loader)

        
        if config["slug"] == "dev":
            
            _write_csv_table(
                df, 
                table["worksheet"],
                config)
            
            df = df[df.dashboard.isin([True, 'TRUE'])].drop(table["public_drop"], 1)
            _write_csv_table(
                df, 
                table["worksheet"],
                config, 
                public=True)

        elif config["slug"] == "prod":
            
            _write_csv_table(
                df, 
                table["worksheet"],
                config)
            
            print(df.shape)
            df = df[df.dashboard.isin([True, 'TRUE'])]
            print(df.shape)
            df = df.drop(table["public_drop"], 1)
            print(df.shape)
            _write_csv_table(
                df, 
                table["worksheet"],
                config, 
                public=True)
        
       

def dummy_2019(config):
    def _daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    start_date = date(config["year"], 1, 1)
    end_date = date(config["year"], 12, 31)
    dates = []
    for single_date in _daterange(start_date, end_date):
        dates.append(
            {
                "year": single_date.year,
                "month": single_date.month,
                "day": single_date.day,
                "dow": single_date.weekday() + 1,
                "sum_length": 0,
            }
        )

    df = pd.DataFrame(dates)

    sql = Path(__file__).cwd() / config["path"]

    query = generate_query(sql, config)

    query_athena(query, config)

    _save_local(df, config, df.columns)

    
    
def save_test_daily(config):

    path = (
        Path.home()
        / "shared"
        / "/".join(config["s3_path"].split("/")[3:])
        / "test"
        / "test_daily_queries"
    )
    
    
    cp_log = f"cp log_{config['test']}.log {path}"
    os.system(cp_log)
    
    df = get_data_from_athena(
            "select * from "
            f"{config['athena_database']}.{config['slug']}_daily_daily"
        )

    df.to_csv(
        path / (config["name"] + '_' + config["test"] + ".csv"), index=False, sep="|"
    )

    
    
def check_existence(config):

    res = get_data_from_athena(
        f"show tables in {config['athena_database']} '{config['slug']}_{config['raw_table']}_{config['name']}'"
    )

    return len(res) > 0
           

def start(config):

    globals()[config["name"]](config)

