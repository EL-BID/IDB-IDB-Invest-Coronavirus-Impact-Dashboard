# analysis libraries
import pandas as pd
import osmpy
from shapely.geometry import box, Polygon, MultiPolygon, GeometryCollection, shape
from shapely import wkt
from shapely.ops import transform
import shapely

# Joao's polygon
# polygon = 'POLYGON((2.0117187499999822 44.38657313925715,-19.433593750000018 19.207272119703983,19.414062499999982 6.904449621538131,64.94140624999999 -3.096801256840523,81.46484374999999 37.21269961002643,45.78124999999998 24.106495997107682,53.69140624999998 51.22054369437158,3.7695312499999822 37.07257833232809,2.0117187499999822 44.38657313925715))'
# geometry = wkt.loads(polygon)

# Preparing geometry ----
polygon = Polygon([(0, 0), (5, 5), (5, 0)])
print(polygon.area)

# Running katana splits ----
geometry = polygon
result = osmpy.core.katana(geometry, osmpy.core.threshold_func, 50, 100)
print(MultiPolygon(result))

# Multipolygon
grid = list()
for polygon in MultiPolygon(result):  # same for multipolygon.geoms
    grid.append(str(polygon))
grid    