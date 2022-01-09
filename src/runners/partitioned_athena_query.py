from multiprocessing.pool import Pool
from functools import partial
from pathlib import Path
from copy import deepcopy
import pyathena
import pandas as pd
import numpy as np
import pytz
from datetime import datetime

import logging
from loguru import logger


log = logging.getLogger(__name__)

from src.utils import (
    break_list_in_chunks,
    to_wkt,
    query_athena,
    generate_query,
    get_data_from_athena,
    delete_s3_path,
    sample_query_weeks,
    get_query_dates,
)


def _all_dates(config, weekly_sample=False):

    if weekly_sample:
        dates = sample_query_weeks(
            config["interval"]["start"], config["interval"]["end"], False,
        )
    else:
        dates = get_query_dates(config["interval"]["start"], config["interval"]["end"])

    return pd.Series(dates, name="date",).apply(lambda x: x.date()).to_frame()


def _get_remaining_dates(data, config):

    existing_dates = get_data_from_athena(
        "select distinct region_slug, "
        "date_parse(concat(cast(year as varchar), '-', cast(month as varchar), '-', cast(day as varchar)), '%Y-%m-%d') date "
        f"from {config['athena_database']}.{config['slug']}_{config['raw_table']}_{config['name']} ",
        config,
    ).assign(date=lambda df: df["date"].apply(lambda x: x.date()))

    dates = _all_dates(config)

    allpossibilities = (
        data.groupby(["region_slug"])
        .apply(lambda x: dates[["date"]])
        .reset_index()
        .drop("level_1", 1)
    )

    return allpossibilities.merge(
        existing_dates, on=["region_slug", "date"], how="left", indicator=True
    ).query('_merge == "left_only"')[["region_slug", "date"]]


def _get_hours(_df, date_format="%Y%m%d"):

    return pd.DataFrame(
        [
            {
                "date_filter": "|".join(
                    [d.strftime(date_format) for d in list(_df["date"])]
                )
            }
        ]
    )


def _choose_time_aggregation(r_dates):

    if len(r_dates) >= 30:
        time_aggregation = "year%Ymonth%m"
    elif len(r_dates) >= 7:
        time_aggregation = "year%Yweek%W"
    else:
        time_aggregation = "year%Ymonth%mday%d"

    r_dates["date_slug"] = r_dates["date"].apply(lambda x: x.strftime(time_aggregation))

    return r_dates


def _add_date_slug(data, remaining_dates, config):

    remaining_dates = (
        remaining_dates.groupby("region_slug")
        .apply(_choose_time_aggregation)
        .reset_index()
        .drop("index", 1)
    )

    remaining_dates = (
        remaining_dates.groupby(["region_slug", "date_slug"])
        .apply(_get_hours)
        .reset_index()
        .drop("level_2", 1)
    )

    return data.merge(remaining_dates, on="region_slug")


def _get_metadata_table(config):

    try:
        return get_data_from_athena(
            "select * from "
            f"{config['athena_database']}.{config['slug']}_analysis_metadata_variation ",
            config,
        )
    except:
        return get_data_from_athena(
            "select * from "
            f"{config['athena_database']}.{config['slug']}_metadata_metadata_ready ",
            config,
        )


def _prepare_to_partition(data, config):

    data = data.to_dict("records")

    for d in data:
        d["partition"] = d["region_slug"]

        if config["name"] == "analysis_daily":
            if d["region_slug"] in config["sampled"]:
                d["dates"] = sample_query_weeks(
                    config["full_2019_interval"]["start"],
                    config["full_2019_interval"]["end"],
                )
            d["p_path"] = deepcopy("country_iso={country_iso}/{partition}".format(**d))

        else:
            d["p_path"] = deepcopy("region_slug={region_slug}/{date_slug}".format(**d))
            d["p_name"] = "{partition}_{date_slug}".format(**d)

    return data


def _region_slug_partition(config):

    data = _get_metadata_table(config)

    if config.get("selected_regions"):

        data = data[data["region_slug"].isin(config.get("selected_regions"))]

    else:

        if config.get("if_exists") == "append":

            # check if table exists
            try:
                skip = get_data_from_athena(
                    "select distinct region_shapefile_wkt from "
                    f"{config['athena_database']}.{config['slug']}_analysis_metadata_variation "
                    "where n_days is not null",
                    config,
                )
            except:
                skip = pd.DataFrame([], columns=["region_shapefile_wkt"])

            data = data[
                ~data["region_shapefile_wkt"].isin(skip["region_shapefile_wkt"])
            ]

            if config["name"] == "analysis_daily":
                data = data[~data["region_slug"].isin(config["cv_exception"])]

    if config.get("mode") == "incremental":

        remaining_dates = _get_remaining_dates(data, config)

        if len(remaining_dates) == 0:
            return None

        data = _add_date_slug(data, remaining_dates, config)

    return _prepare_to_partition(data, config)


def historical_2019(config):

    return _region_slug_partition(config)


def historical_2020(config):

    return _region_slug_partition(config)


def daily(config):

    logger.info(config)
    return _region_slug_partition(config)


def sample_2019(config):

    return _region_slug_partition(config)


def analysis_daily(config):

    return _region_slug_partition(config)


def country_cities(config):

    regions = get_data_from_athena(
        "select distinct region_slug "
        f"from {config['athena_database']}.{config['slug']}_{config['raw_table']}_{config['from_table']}",
        config,
    )["region_slug"].tolist()

    return [
        {"p_name": r, "p_path": f"region_slug={r}", "partition": r, "region_slug": r}
        for r in regions
    ]


def country_cities_2020(config):

    return country_cities(config)


def country_cities_2019(config):

    return country_cities(config)


def grid(config):

    regions = list(
        get_data_from_athena(
            "select distinct region_slug from "
            f"{config['athena_database']}.{config['slug']}_metadata_metadata_prepare "
            "where grid = 'TRUE'"
            f"""or region_slug in ('{"','".join(config['selected_regions'])}')""",
            config,
        )["region_slug"]
    )

    return [
        {"p_name": r, "p_path": f"region_slug={r}", "partition": r, "region_slug": r}
        for r in regions
    ]


def grid_2020(config):

    return grid(config)


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

            delete_s3_path(query["p_path"], query["config"])

            query_athena(query["drop"], query["config"])

            query_athena(query["make"], query["config"])

            query_athena(query["drop"], query["config"])
            break

        except Exception as e:
            if query["config"]["verbose"]:
                print(e)
            if i == query["config"]["n_tries"] - 1:
                raise e
            continue


def check_existence(config):

    res = get_data_from_athena(
        f"show tables in \
                    {config['athena_database']} '{config['slug']}_{config['raw_table']}_{config['name']}'"
    )

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

    partitions = globals()[config["name"]](config)

    if partitions is None:
        return

    for partition in partitions:

        config.update(deepcopy(partition))

        queries.append(
            dict(
                make=generate_query(query_path, config),
                drop=f"drop table if exists {config['athena_database']}.{config['slug']}_{config['raw_table']}_{config['name']}_{config['p_name']}",
                config=config,
                p_path=deepcopy(config["p_path"]),
                partition=config["partition"],
            )
        )

    with Pool(config["number_of_athena_jobs"]) as p:
        p.map(partial(perform_query), queries)


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
        return current_millis["current_millis"][0] != config.get("current_millis")
    else:
        return True


def start(config):

    sql_path = Path(__file__).cwd() / config["path"]

    # create table
    if should_create_table(config):
        query_athena(generate_query(sql_path / "create_table.sql", config), config)

    config["force"] = False

    # fill table
    partition_query(sql_path / "fill_table.sql", config)

    # partition
    sql = generate_query(sql_path / "partition.sql", config)

    query_athena(sql, config)
