
# LIBRARIES
import pandas as pd
import geopandas as gpd
from datetime import datetime
import osmpy
from shapely.geometry import box, Polygon, MultiPolygon, GeometryCollection, shape
from shapely import wkt
from shapely.ops import transform
# import shapely

## FUNCTIONS
def intersection_func(line, geometry):
    result = geometry.intersection(wkt.loads(line)).is_empty == False
    return(int(result))

def threshold_density_func(geometry, threshold_value):
    """Compares the threshold values with the number of lines"""
    
    print('Running')
    
    # Intersection of lines within square
    times = [intersection_func(line, geometry) for line in df_lines.line_wkt]
    total_lines = df_lines.count_lines
    
    # Total lines in square
    inter = sum([times[x]*total_lines[x] for x in range(len(total))])
    total = sum(total_lines)
    
    print(f"Intersection {intersection}")
    print(f"Total lines {total}")
    print(f"Proportion {intersection/total}")
    
    return (inter/total) < (threshold_value/total)


## POLIGONOS

# LA's square
# polygon = 'POLYGON((-129.454 37.238,-90.781 27.311,-67.117 20.333,-68.721 17.506,-23.765 -9.114,-65.601 -60.714,-126.421 -23.479,-129.454 37.238))'
#geometry = wkt.loads(polygon)



## RUNNING

# Date run ----
cm = str(datetime.today().strftime("%Y%m%d%H%m%s"))
print(cm)

# Preparing geometry ----
# - Latin america BID
polygon = 'POLYGON((-129.454 37.238,-90.781 27.311,-67.117 20.333,-68.721 17.506,-23.765 -9.114,-65.601 -60.714,-126.421 -23.479,-129.454 37.238))'
geometry = wkt.loads(polygon)

# Running katana splits ----
result = osmpy.core.katana(geometry, 
                           threshold_func = threshold_density_func, 
                           threshold_value = 110000, 
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
