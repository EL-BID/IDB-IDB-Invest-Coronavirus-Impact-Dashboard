import pandas as pd
import boto3
from shapely import wkt
import geojson
from copy import deepcopy
from shapely import affinity
from babelgrid import Babel

from runners.create_local_table_athena import _save_local
from runners import basic_athena_query
from operators.athena import insert_into, create_table
from utils import get_data_from_athena

from loguru import logger


def _reescale(s, factor=1.1):

    return affinity.scale(wkt.loads(s), factor, factor).to_wkt()


def _parent(tile, resolution):

    while tile.resolution > resolution:
        tile = tile.to_parent()

    return tile.tile_id


def _get_cell(wkt, resolution, parent_resolution=1):

    temp = pd.DataFrame(
        [
            {
                "tile": t,
                "id": t.tile_id,
                "resolution": t.resolution,
                "wkt": t.geometry.wkt,
            }
            for t in Babel("h3").polyfill(wkt.values[0], resolution)
        ]
    )

    temp["id_parent"] = temp["tile"].apply(lambda x: _parent(x, parent_resolution))
    temp["group"] = (temp.index / 100).astype(int)
    temp = temp.drop("tile", 1)
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

    print(config['region_def'])
    
    metadata = get_data_from_athena(
        "select region_slug, region_shapefile_wkt from "
        f"{config['athena_database']}.{config['slug']}_metadata_metadata_prepare "
        f"where region_slug = '{config['region_def']}'",
        #f"""or region_slug in ('{"','".join(config['selected_regions'])}')""",
        config,
    )

    metadata["wkt"] = metadata["region_shapefile_wkt"].apply(_reescale)
    # metadata["geojson"] = metadata["wkt_reescaled"].apply(_wkt_to_geojson)

    grid = (
        metadata.groupby("region_slug")["wkt"]
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
