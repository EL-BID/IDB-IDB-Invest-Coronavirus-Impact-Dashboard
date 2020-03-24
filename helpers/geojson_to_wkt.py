import sys
import geojson
from shapely.geometry import shape
import pandas as pd
from pathlib import Path
from fire import Fire

def convert(path, region_type):
    converted = []
    for p in Path(path).glob('*.GeoJson'):
        d = geojson.load(open(p, 'r'))
        converted.append(dict(
            country_name=d['properties']['name'],
            country_iso=d['properties']['alltags']['ISO3166-1'],
            region_slug='_'.join([region_type] + d['properties']['name'].lower().split(' ')),
            region_name=d['properties']['name'],
            region_type=region_type,
            dashboard='TRUE',
            population=d['properties']['alltags'].get('population'),
            timezone=d['properties']['alltags'].get('timezone'),
            region_shapefile_wkt=shape(d['geometry']).simplify(0.05, preserve_topology=False).wkt
        ))
    pd.DataFrame(converted).to_csv(Path(path) / 'converted.csv', index=False)

if __name__ == "__main__":

    Fire(convert)