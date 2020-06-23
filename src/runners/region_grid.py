import pandas as pd
import boto3
from h3 import h3
from shapely import wkt
import geojson
from copy import deepcopy
from shapely import affinity

from runners.create_local_table_athena import _save_local
from runners import basic_athena_query
from operators.athena import insert_into, create_table
from utils import get_data_from_athena


def _reescale(s, factor=1.1):

    return affinity.scale(wkt.loads(s), factor, factor).to_wkt()


def _wkt_to_geojson(s):
    return geojson.Feature(geometry=wkt.loads(s), properties={}).geometry


def _to_wkt(x):
    x = x + [x[0]]
    return "polygon" + str(x).replace("], [", ",").replace(", ", " ").replace(
        "[", "("
    ).replace("]", ")")


def _get_cell(gjson, resolution, parent_resolution=1):

    temp = pd.Series(
        list(h3.polyfill(gjson.values[0], resolution, True)), name="id"
    ).to_frame()
    temp["resolution"] = resolution
    temp["wkt"] = temp["id"].apply(lambda x: _to_wkt(h3.h3_to_geo_boundary(x, True)))
    temp["id_parent"] = temp["id"].apply(
        lambda x: h3.h3_to_parent(x, res=parent_resolution)
    )
    temp["group"] = (temp.index / 100).astype(int)
    return temp


def get_cells(gjson, config):

    return pd.concat(
        [
            _get_cell(
                gjson,
                config["granular_resolution"],
                parent_resolution=config["coarse_resolutions"],
            ),  # granular grid
            _get_cell(
                gjson, config["coarse_resolutions"], config["coarse_resolutions"]
            ),
        ]
    )


def resolutions(config):

    metadata = get_data_from_athena(
        "select region_slug, region_shapefile_wkt from "
        f"{config['athena_database']}.{config['slug']}_metadata_metadata_prepare "
        "where grid = 'TRUE'",
        config,
    )

    metadata["wkt_reescaled"] = metadata["region_shapefile_wkt"].apply(_reescale)
    metadata["geojson"] = metadata["wkt_reescaled"].apply(_wkt_to_geojson)

    grid = (
        metadata.groupby("region_slug")["geojson"]
        .apply(lambda x: get_cells(x, config))  # Get h3 ids and wkts
        .reset_index()
    )

    create_table.from_local(grid, config, wrangler=True)


def coarse(config):

    insert_groups = get_data_from_athena(
        'select distinct region_slug, "group" from '
        f"{config['athena_database']}.{config['slug']}_grid_resolutions "
        "where resolution = 7",
        config,
    ).to_dict("records")

    insert_into.start(config, insert_groups)


def coarse_2020(config):

    return coarse(config)


def check_existence(config):

    res = get_data_from_athena(
        f"show tables in {config['athena_database']} '{config['slug']}_{config['raw_table']}_{config['name']}'"
    )

    return len(res) > 0


def start(config):

    globals()[config["name"]](config)
