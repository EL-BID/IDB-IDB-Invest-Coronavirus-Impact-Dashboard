
# LIBRARIES
import pandas as pd
import geopandas as gpd
from datetime import datetime
import osmpy
from shapely.geometry import box, Polygon, MultiPolygon, GeometryCollection, shape
from shapely import wkt
from shapely.ops import transform
import shapely
# from shapely.wkt import loads

## FUNCTIONS

def intersection_func(line, geometry):
    line_wkt = wkt.loads(line)
    result = geometry.intersection(line_wkt).is_empty == False
    return(int(result))

def threshold_density_func(geometry, threshold_value):
    """Compares the threshold values with the number of lines"""
    
    print('Running')
    
    times = [intersection_func(line, geometry) for x in df_lines.line_wkt]
    intersection = sum(times)
    total = len(times)
    
    print(intersection)
    print(total)
    print(intersection/total)
    
    return (intersection/total) < (threshold_value/total)


## POLIGONOS

# LA's square
# polygon = 'POLYGON((-129.454 37.238,-90.781 27.311,-67.117 20.333,-68.721 17.506,-23.765 -9.114,-65.601 -60.714,-126.421 -23.479,-129.454 37.238))'
#geometry = wkt.loads(polygon)



## RUNNING

# Date run ----
cm = str(datetime.today().strftime("%Y%m%d%H%m%s"))
print(cm)

# Preparing geometry ----
polygon = 'POLYGON ((-77.10205078124999 -13.004557745339769, -72.158203125 -13.004557745339769, -72.158203125 -8.90678000752024, -77.10205078124999 -8.90678000752024, -77.10205078124999 -13.004557745339769))'
geometry = wkt.loads(polygon)

# Running katana splits ----
result = osmpy.core.katana(geometry, 
                           threshold_func = threshold_density_func, 
                           threshold_value = 800, 
                           count = 100)
print(len(MultiPolygon(result).geoms))
#print(MultiPolygon(result))

# Multipolygon ----
grid = list()
for polygon in MultiPolygon(result):  # same for multipolygon.geoms
    grid.append(str(polygon))


# Export to csv ----
outdf = gpd.GeoDataFrame(columns=['geometry'])
outdf['geometry'] = grid
outdf.to_csv(f"~/private/projects/geo_grid_area_{cm}.csv")
