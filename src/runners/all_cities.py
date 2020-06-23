import awswrangler as wr
import pandas as pd
import boto3
from h3 import h3
from shapely import wkt
import geojson
from copy import deepcopy
from shapely import affinity

from src.runners import basic_athena_query
from src.operators.athena import insert_into, create_table
from src.utils import get_data_from_athena


def _wkt_to_geojson(s):
    return geojson.Feature(geometry=wkt.loads(s), properties={}).geometry


def _to_wkt(x):
    x = x + [x[0]]
    return "polygon" + str(x).replace("], [", ",").replace(", ", " ").replace(
        "[", "("
    ).replace("]", ")")


def _reescale(s, factor=1.1):

    return affinity.scale(wkt.loads(s), factor, factor).to_wkt()


def _bigger_polygon(poly):
    return max(
        [(i.to_wkt(), i.area) for i in list(wkt.loads(poly))], key=lambda x: x[1]
    )[0]


def _get_cell(gjson, resolution, parent_resolution=1, keep_wkt=False):

    inverse_coords = True

    temp = pd.Series(
        list(h3.polyfill(gjson.values[0], resolution, inverse_coords)), name="id"
    ).to_frame()

    if len(temp) == 0:
        temp = pd.Series(
            [
                h3.geo_to_h3(
                    **dict(zip(("lng", "lat"), gjson.values[0]["coordinates"][0][0])),
                    res=resolution,
                )
            ],
            name="id",
        ).to_frame()

        inverse_coords = True

    temp["resolution"] = resolution

    temp["id_parent"] = temp["id"].apply(
        lambda x: h3.h3_to_parent(x, res=parent_resolution)
    )
    temp["group"] = (temp.index / 100).astype(int)

    if keep_wkt:
        temp["wkt"] = _to_wkt(gjson.values[0]["coordinates"][0])
    else:
        temp["wkt"] = temp["id"].apply(
            lambda x: _to_wkt(h3.h3_to_geo_boundary(x, inverse_coords))
        )

    return temp


def country_resolutions(config):

    metadata = get_data_from_athena(
        "select region_slug, region_shapefile_wkt from "
        f"{config['athena_database']}.{config['slug']}_metadata_metadata_prepare "
        f"""where region_slug in ('{"','".join([j['region_slug'] for j in config['jobs']])}')""",
        config,
    )

    country_grid = (
        metadata.set_index("region_slug")["region_shapefile_wkt"]
        .apply(_bigger_polygon)
        .apply(_reescale)
        .apply(_wkt_to_geojson)
        .reset_index()
        .groupby("region_slug")["region_shapefile_wkt"]
        .apply(
            lambda x: _get_cell(
                x, config["coarse_resolutions"], config["coarse_resolutions"]
            )
        )
        .reset_index()
        .drop("level_1", 1)
    )

    city_grid = pd.concat(
        [
            (
                pd.read_csv("data/support_tables/all_cities/" + job["file"], sep="|")
                .set_index("region_name")["region_shapefile_wkt"]
                .apply(lambda x: wkt.loads(x).convex_hull.to_wkt())
                .apply(_wkt_to_geojson)
                .reset_index()
                .groupby("region_name")["region_shapefile_wkt"]
                .apply(
                    lambda x: _get_cell(
                        x,
                        config["coarse_resolutions"],
                        config["coarse_resolutions"],
                        keep_wkt=True,
                    )
                )
                .reset_index()
                .drop(["level_1", "id_parent"], 1)
                .rename(columns={"region_name": "id", "id": "id_parent"})
                .assign(region_slug=job["region_slug"], resolution=10)
            )
            for job in config["jobs"]
        ]
    )

    grid = pd.concat([city_grid, country_grid])
    grid["id_parent"] = grid["id_parent"].astype(str)
    grid["id"] = grid["id"].astype(str)

    create_table.from_local(grid, config, wrangler=True)


def country_coarse(config):

    insert_groups = get_data_from_athena(
        'select distinct region_slug, "group" from '
        f"{config['athena_database']}.{config['slug']}_cities_country_resolutions "
        "where resolution = 3",
        config,
    ).to_dict("records")

    insert_into.start(config, insert_groups)


def country_coarse_2020(config):

    return country_coarse(config)


def country_coarse_2019(config):

    return country_coarse(config)


def check_existence(config):

    res = get_data_from_athena(
        f"show tables in {config['athena_database']} '{config['slug']}_{config['raw_table']}_{config['name']}'"
    )

    return len(res) > 0


def start(config):

    globals()[config["name"]](config)
