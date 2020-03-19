from h3 import h3
from utils import generate_query, query_athena, to_wkt, get_geometry
from multiprocessing.pool import Pool
from functools import partial

import logging
log = logging.getLogger(__name__)

def split_outer_polygon(outer_polygon, resolution=6):
    """Splits initial geometry in H3 Hexagon grids and
    translates array based geometry to WKT.

    Parameters
    ----------
    outer_polygon : list of lists
        initial geometry in array format
    resolution : int, optional
        H3 Hexagon grid resolution, by default 6
    
    Returns
    -------
    list of strings
        H3 hexagons at requested resolution for outer_polygon area as 
        WKT strings 
    """
    
    swipes = lambda x: [x[1], x[0]]
    to_wkt = lambda x: 'polygon' + str(x).replace('], [', ',')\
                                            .replace(', ', ' ')\
                                            .replace('[', '(')\
                                            .replace(']', ')')
    
    
    set_hexagons = h3.polyfill(geo_json=outer_polygon, res=resolution, geo_json_conformant=True)
    
    hexagons = list(map(lambda x: {'id': x, 
                                   'geometry': h3.h3_to_geo_boundary(x) + 
                                               [h3.h3_to_geo_boundary(x)[0]]}, 
                        list(set_hexagons)))
    
    hexagons = list(map(lambda x: {'id': x['id'], 'geometry': list(map(swipes, x['geometry']))}, hexagons))
    
    return list(map(lambda x: {'id': x['id'], 'geometry': to_wkt(x['geometry'])}, hexagons))



def perform_query(query):
    """Simply calls Athena and logs the exceptions.
    
    Parameters
    ----------
    query : dict
        dict with two objects, `make` and `drop`. The first to create
        a table and the second to drop the same table.
    """

    query_athena(query['make'], query['config'])

    query_athena(query['drop'], query['config'])
    

def query(query_path, config):
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

    if 'merged' in query_path:

        internal_polygons = split_outer_polygon(get_geometry(config['geometry_path']), 
                            resolution=config['polygon_resolution'])

        for polygon in internal_polygons:

            config.update(dict(
                polygon_id=polygon['id'],
                polygon_geometry=polygon['geometry']
            ))

            queries.append(
                dict(make=generate_query(query_path, config),
                    drop=
                    f"drop table {config['athena_database']}.{config['slug']}_merged_segments_{config['polygon_id']}",
                    config=config
                            ))

    elif 'unbalanced' in query_path:

        for partition in range(0, 100):

            config.update(dict(
                partition=partition
            ))

            queries.append(
                dict(make=generate_query(query_path, config),
                    drop=
                    f"drop table {config['athena_database']}.\
                        {config['slug']}_unbalanced_panel_{config['partition']}",
                    config=config
            ))

    with Pool(config['number_of_athena_jobs']) as p:
        p.map(partial(perform_query), queries)
