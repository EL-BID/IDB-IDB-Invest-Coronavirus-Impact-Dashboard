# analysis libraries
import pandas as pd
import geopandas as gpd
from datetime import datetime
import osmpy
from shapely.geometry import box, Polygon, MultiPolygon, GeometryCollection, shape
from shapely import wkt
from shapely.ops import transform
import shapely



# ################## #

# Joao's polygon
# polygon = 'POLYGON((2.0117187499999822 44.38657313925715,-19.433593750000018 19.207272119703983,19.414062499999982 6.904449621538131,64.94140624999999 -3.096801256840523,81.46484374999999 37.21269961002643,45.78124999999998 24.106495997107682,53.69140624999998 51.22054369437158,3.7695312499999822 37.07257833232809,2.0117187499999822 44.38657313925715))'

# LA's square
#polygon = 'POLYGON ((-127.265625 34.30714385628804, -128.671875 -56.94497418085159, -28.4765625 -57.70414723434192, -29.8828125 16.97274101999902, -84.72656249999999 25.48295117535531, -116.71874999999999 35.746512259918504, -127.265625 34.30714385628804))'
# geometry = wkt.loads(polygon)

# ################## #



# Date run ----
cm = str(datetime.today().strftime("%Y%m%d%H%m"))
print(cm)

# Preparing geometry ----
geometry = Polygon([(0, 0), (5, 5), (5, 0)])
print(geometry.area)

# Running katana splits ----
result = osmpy.core.katana(geometry, 
                           threshold_func = osmpy.core.threshold_func, 
                           threshold_value = 50, 
                           count = 30)
print(len(MultiPolygon(result).geoms))
print(MultiPolygon(result))

# Multipolygon
grid = list()
for polygon in MultiPolygon(result):  # same for multipolygon.geoms
    grid.append(str(polygon))
grid    


# Export to csv
outdf = gpd.GeoDataFrame(columns=['geometry'])
outdf['geometry'] = grid
outdf.to_csv(f"/home/soniame/private/testing/geo_grid_area_{cm}.csv")