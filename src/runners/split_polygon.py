
# LIBRARIES
import pandas as pd
import numpy as np
import geopandas as gpd
from datetime import datetime

from shapely.geometry import box, Polygon, MultiPolygon, GeometryCollection, shape
from shapely import wkt
from shapely.ops import transform

from babelgrid import Babel

from multiprocessing.pool import Pool
from functools import partial


from loguru import logger


## FUNCTIONS
def _get_lines(update_data = False):

    logger.info('Lines')

    # Download data from Athena
    if update_data:
        logger.debug("Downloading lines")
        
        conn = utils.connect_athena(path='configs/athena.yaml')
        qry = f"""
            select line_wkt, count(line_wkt) as count_lines
            from spd_sdv_waze_corona.raw_sample_jams
            group by line_wkt"""
        df_lines = pd.read_sql_query(qry, conn)
        df_lines.to_csv('/home/soniame/private/line_wkt_count_202010712.csv', index=False)
    # Read current table
    else:
        logger.debug("Reading lines")             
        df_lines = pd.read_csv('/home/soniame/private/projects/corona_geo_id/lines/line_wkt_count_202010712.csv')
    
    logger.debug(f"Lines: {len(df_lines)}")
                     
    return(df_lines)





def _line_to_coarse(line, tiles):
    
    # list with logical value of grid tiles intersection per line
    # Total length is the number of tiles
    inter_list = list()
    for tile in tiles:
        # tile geometry
        geom = tile.geometry.shapely
        # intersection of tile geometry with line
        inter_list.append(geom.intersection(wkt.loads(line)).is_empty == False)

    # wkt assigned to each line
    if sum(inter_list) == 0:
        # In case there's no intersection
        pos = None
        t_wkt = ""
    else:
        pos = np.where(inter_list)[0].tolist()[0]  
        t_wkt = tiles[pos].geometry.wkt

    result = {'line': line, 'coarse_wkt': t_wkt}        
        
    return(result)


def _coarse_grid(df_lines, tiles):
    
    logger.info('Coarse grid')
    
    # Lines done previously
    prev = pd.read_csv("/home/soniame/private/projects/corona_geo_id/coarse_grid/coarse_id.csv"). \
        rename(columns = {'line':'line_wkt'})
    logger.debug(f'PL: {len(prev)}') # preview lines  
    
    # Elimination of lines already done
    df_merge = df_lines.merge(prev, how='left')
    df_merge = df_merge[df_merge.coarse_wkt.isnull() == True]
    logger.debug(f'Lines done: {len(df_lines) - len(df_merge)}') # new lines
    
    # Final lines
    lines = df_merge.line_wkt
    logger.debug(f'NL: {len(lines)}') # new lines
    
    # Matching lines per tile
    with Pool(10) as p:
        r = p.map(partial(_line_to_coarse, tiles = tiles), lines)
        
    df_coarse = pd.DataFrame(r)   
    logger.debug(f"UL: {df_coarse.shape[0]}") # update lines
    
    df_coarse.to_csv("/home/soniame/private/projects/corona_geo_id/coarse_grid/coarse_id_new.csv", index = False)
    
    return None


def _intersection_func(line, geometry):
    
    # TODO: create coarse grid
    result = geometry.intersection(wkt.loads(line)).is_empty == False
    return(int(result))


def _threshold_density_func(geometry, threshold_value):
    """Compares the threshold values with the number of jams
    Parameters
    ----------
    geometry: Polygon or MultiPolygon
        Geometry to intersect lines
    threshold_value: number
        Max percentage of jams pero square
    """
    
    print('Running')
    
    # Intersection of lines within square
    # TODO: change to global variable
    # df_lines = pd.read_csv('/home/soniame/private/line_wkt_count_202010701.csv')
    # times = [_intersection_func(line, geometry) for line in df_lines.line_wkt]
    total_lines = df_lines.count_lines
    
    # Total lines in square
    inter = sum([times[x]*total_lines[x] for x in range(len(total_lines))])
    total = sum(total_lines)
    
    print(f"Intersection {inter}")
    print(f"Total lines {total}")
    print(f"Proportion {inter/total}")
    
    return (inter/total) < (threshold_value)

def threshold_func(geometry, threshold_value):
    """Compares the threshold values with the polygon area"""
    return geometry.area < threshold_value

def katana(geometry, threshold_func, threshold_value, max_number_tiles, number_tiles=0):
    """Splits a geometry in tiles forming a grid given a threshold function and
    a maximum number of tiles.
    
    Parameters
    ----------
    geometry: Polygon or MultiPolygon
        Initial geometry
    threshold_func: function
        Calculete how many segments or km or data points exists in geometry
        Should return True or False given a threshold_value.
        Let's say you want to stop dividing polygons when you reach 10k unique segments,
        then, you should return True when the geometry has < 10k segmnets.
    threshold_value: number
        Whatever value you set as the max of the quantity you want in each tile
    number_tiles: int
        Number of tiles, defaults to 0.
    max_number_tiles: int
        Maximum number of tiles
    
    Return
    ------
    geometry: MultiPolygon
        Initial geometry divided in tiles
        
    KUDOS https://snorfalorpagus.net/blog/2016/03/13/splitting-large-polygons-for-faster-intersections/
    """
    bounds = geometry.bounds
    width = bounds[2] - bounds[0]
    height = bounds[3] - bounds[1]
    if threshold_func(geometry, threshold_value) or number_tiles == max_number_tiles:
        # either the polygon is smaller than the threshold, or the maximum
        # number of recursions has been reached
        return [geometry]
    if height >= width:
        # split left to right
        a = box(bounds[0], bounds[1], bounds[2], bounds[1] + height / 2)
        b = box(bounds[0], bounds[1] + height / 2, bounds[2], bounds[3])
    else:
        # split top to bottom
        a = box(bounds[0], bounds[1], bounds[0] + width / 2, bounds[3])
        b = box(bounds[0] + width / 2, bounds[1], bounds[2], bounds[3])
    result = []
    for d in (
        a,
        b,
    ):
        c = geometry.intersection(d)
        if not isinstance(c, GeometryCollection):
            c = [c]
        for e in c:
            if isinstance(e, (Polygon, MultiPolygon)):
                result.extend(katana(e, threshold_func, threshold_value, number_tiles + 1))
    if count > 0:
        return result
    # convert multipart into singlepart
    final_result = []
    for g in result:
        if isinstance(g, MultiPolygon):
            final_result.extend(g)
        else:
            final_result.append(g)
    return final_result
    

    
def _katana_grid(geometry):
    
    logger.info('Katana grid')
    
    result = katana(geometry, 
                    threshold_func = _threshold_density_func, 
                    threshold_value = .01, 
                    max_number_tiles = 100)
    print(len(MultiPolygon(result).geoms))
    print(MultiPolygon(result))

    # Multipolygon ----
    grid = list()
    for polygon in MultiPolygon(result):  # same for multipolygon.geoms
        grid.append(str(polygon))


    # Export to csv ----
    outdf = gpd.GeoDataFrame(columns=['geometry'])
    outdf['geometry'] = grid
    outdf.to_csv(f"~/private/geo_id_polygon/geo_grid_area_{cm}.csv")

    

## RUNNING
def create_squares():
    
    # Date run ----
    cm = str(datetime.today().strftime("%Y%m%d%H%m%s"))
    print(cm)

    # Polygon geometry definition ----
    # - Latin america BID
    polygon = 'POLYGON((-129.454 37.238,-90.781 27.311,-67.117 20.333,-68.721 17.506,-23.765 -9.114,-65.601 -60.714,-126.421 -23.479,-129.454 37.238))'
    geometry = wkt.loads(polygon)

    # Lines 
    df_lines = _get_lines()
    
    # Coarse grid
    tiles = Babel('h3').polyfill(geometry, resolution=1)
    logger.debug(f"Tiles: {len(tiles)}")
    
    # Lines to coarse grid ----
    _coarse_grid(df_lines, tiles)
    
    # Running katana splits ----
    # _katana_grid

create_squares()