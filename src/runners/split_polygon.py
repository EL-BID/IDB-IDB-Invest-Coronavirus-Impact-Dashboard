
# LIBRARIES
import pandas as pd
import geopandas as gpd
from datetime import datetime

from shapely.geometry import box, Polygon, MultiPolygon, GeometryCollection, shape
from shapely import wkt
from shapely.ops import transform

from loguru import logger


## FUNCTIONS
def line_to_coarse(line):
    logger.debug(line)
    
    inter_list = list()
    for tile in tiles:
        geom = tile.geometry.shapely
        inter_list.append(geom.intersection(wkt.loads(line)).is_empty)

    result = {'line': line, 'coarse_grid': tiles[inter_list == False].geometry.wkt}
    return(result)

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
    df_lines = pd.read_csv('/home/soniame/private/line_wkt_count_202010701.csv')
    times = [_intersection_func(line, geometry) for line in df_lines.line_wkt]
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
    df_lines = pd.read_csv('/home/soniame/private/projects/corona_geo_id/lines/line_wkt_count_202010701.csv')
    logger.debug(f"Liles: {len(df_lines)}")
    
    # Coarse grid
    tiles = Babel('h3').polyfill(geometry, resolution=1)
    logger.debug(f"Tiles: {len(tiles)}")
    
    # Lines to coarse grid ----
    with Pool(5) as p:
        r = p.map(partial(line_to_coarse), df_lines.line_wkt)
    df_coarse = pd.DataFrame(r)    
    
    # Running katana splits ----
    result = katana(geometry, 
                    threshold_func = _threshold_density_func, 
                    threshold_value = .01, 
                    max_number_tiles = 100)
    # print(len(MultiPolygon(result).geoms))
    # print(MultiPolygon(result))

    # Multipolygon ----
    grid = list()
    for polygon in MultiPolygon(result):  # same for multipolygon.geoms
        grid.append(str(polygon))


    # Export to csv ----
    outdf = gpd.GeoDataFrame(columns=['geometry'])
    outdf['geometry'] = grid
    outdf.to_csv(f"~/private/geo_id_polygon/geo_grid_area_{cm}.csv")


