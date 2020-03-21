from fire import Fire
from pathlib import Path
import yaml
import shapely.wkt
import geojson
from datetime import datetime
import sqlite3
import pandas as pd

from core import core
from utils import timed_log, get_config, get_data_from_athena, safe_create_path

def from_wkt_to_geojson(polygon):

    g1 = shapely.wkt.loads(polygon)

    return geojson.Feature(geometry=g1, properties={})

def create_geo_file(city, config, pathname):
    
    if config['city_or_metro'] == 'metro':
    
        polygon = city['fua_shapefile_wkt']

    elif config['city_or_metro'] == 'city':
    
        polygon = city['city_shapefile_wkt']
    
    polygon = from_wkt_to_geojson(polygon)
    
    geojson.dump(polygon, open(pathname / (city['city_slug'] + '.geojson'), 'w'))

def create_config_file(city, config, pathname):
    
    general_config = get_config('configs/config-template.yaml')
    
    general_config.update(**config['dates'])
    
    general_config.update({'slug': city['city_slug'],
                        'timezone': city['timezone'],
                        'country_name': city['country_name'],
                        'country_iso': city['country_iso'],
                        })
    
    yaml.dump(general_config, open(pathname / (city['city_slug'] + '.yaml'), 'w'))

def create_files(cities, city_slug, config, save_path):
    
    city = cities.query(f'city_slug == "{city_slug}"').drop_duplicates('city_slug').iloc[0]
        
    pathname = save_path / config['job_name'] / datetime.now().strftime('%Y%m%d')
    safe_create_path(pathname)
    
    create_geo_file(city, config, pathname)
    create_config_file(city, config, pathname)
    yaml.dump(config, open(pathname / ('config-database.yaml'), 'w'))

    return pathname


class Run(object):

    def __init__(self,
        drop_tables=False,
        flush_temp_tables=False,
        queries_to_run=[],
        verbose=False,
        dryrun=False,
        force_downstream=True,
        runall=False,
        force=False,
        report=True,
        n_tries=5,
        run_queries=True,
        document=False,
        dependency_graph_path='configs/dependency_graph.yaml',
        tables_to_drop=['estimations_network', 'estimations_open', 'estimations_open_winsorized',
    'estimations_roadtype', 'lines_to_segments', 'segments',
    'unbalanced_panel_csv', 'estimations_accuracy', 'estimations_accuracy_level',
    'estimations_accuracy_roadtype']):
        """Entrypoint function. 

        1. loads the configuration file
        2. cleans the table space with same slug, if requested
        3. sorts the queries by preffix number
        4. run through the queries in order

        In step 4, it calls a partitioned.query for query numbers explicited in
        config.yaml under the key {{ partitioned_query }}. 

        All the outputs are athena tables that are stored in a S3 path explicited
        in config.yaml with the following path structure

            `{{ s3_path }}/{{ slug }}/temp_tables/{{ table_name }}` 
        
        and in an Athena database with tables names as

            `{{ slug }}_{{ table_name }}`
        
        
        Parameters
        ----------
        drop_tables : bool, by default False
            Whether you want to drop tables with the same `slug`, by default True
        queries_to_run: list, by default []
            Number of the queries to be ran, e.g. [3,4,5]
        verbose: bool, by default False
            Prints more prolific statements about the code
        dryrun: bool, by default False
            Runs code without querying athena
        runall: bool, by default False
            Runs all pipeline including query 0. This is *costly*
        report: bool, by default True
            Writes report to shared folder at `{{ slug }}/support_files/auto-report.html`
        tables_to_drop: list
            tables to drop from Athena. Empty list if not to drop any table
        """

        self.drop_tables = drop_tables 
        self.flush_temp_tables = flush_temp_tables 
        self.queries_to_run = queries_to_run        
        self.verbose = verbose 
        self.dryrun = dryrun 
        self.runall = runall 
        self.force_downstream = force_downstream
        self.report = report 
        self.tables_to_drop = tables_to_drop 
        self.force = force
        self.dependency_graph_path = dependency_graph_path
        self.run_queries = run_queries
        self.n_tries = n_tries
        self.document = document

        self.args = vars(self)

    def single(self, config_path='configs/config-template.yaml'):
        """Run the pipeline for a single region
        
        Parameters
        ----------
        config_path : str, optional
            config.yaml path, by default 'configs/config.yaml'
        geometry_path : str, optional
            geometry.json path, by default 'configs/geometry.yaml'
        """

        self.args.update(dict(
            config=get_config(config_path),
            config_path=config_path
        ))

        if self.verbose: 
            print(config_path)
            print(self.args['config'])

        # try:
        with timed_log(name='Start process', config=self.args['config'], time_chunk='minutes', 
                        force=self.args['force']):
                pass
                
        with timed_log(name='Full process', config=self.args['config'], time_chunk='minutes', 
                        force=self.args['force']):
            core(self.args)
        
        # except Exception as e:
        #     print(e)
        #     pass

    def _run_batch(self, slugs, folder, geometry_type, exclude):

        for slug in slugs:

            if slug not in exclude:
                
                self.single(
                    config_path=str(folder / (slug + '.' + 'yaml')),
                    geometry_path=str(folder / (slug + '.' + geometry_type))
                    )


    def batch(self, 
        folder, 
        config_name='config-template.yaml',
        geometry_type='geojson',
        exclude=[]):
        """Run the pipeline for several regions.

        If you have just one config for several geometries, make sure that --config_name 
        and the config file names match

        Else, if you have a folder with pair of config and geometry, then make sure that 
        --config_name does not match with any of the files.

        Parameters
        ----------
        folder : string
            Path to folder with files to be tun
        config_name : str, optional
            General config file for batch, by default 'config-template.yaml'
        geometry_type : str, optional
            Type of geometry file. Accepts geojson and json, by default 'geojson'
        exclude : list, optional
            Files that should be skipped by pipeline, by default []
        """

        batch_folder = (Path().home() / 'projects' / 'waze_data_normalization' / folder)

        config_template_path = batch_folder / config_name

        # Just geometries with one config template
        if config_template_path.exists():
            
            config_template = yaml.load(config_template_path.open('r'))
            
            slugs = list(map(lambda g: g.stem, 
                        batch_folder.glob(f'*.{geometry_type}')))
            
            for slug in slugs:

                config_template['slug'] = slug
                
                yaml.dump(config_template, (batch_folder / (slug + '.yaml')).open('w'))
            
            self._run_batch(slugs, Path(folder), geometry_type, exclude)

        # Config and template pair
        else:
            
            slugs = map(lambda g: g.stem, batch_folder.glob(f'*.{geometry_type}'))

            self._run_batch(slugs, Path(folder), geometry_type, exclude)


    def database(self,
        batch_file,
        config_path='config-template.yaml',
        save_path='batch/database/'):

        db_config = yaml.load(open(batch_file))

        con = sqlite3.connect(
            str(Path().home() / 'shared/spd-sdv-omitnik-waze/indexes/*cities/cities.db'))

        cities = pd.read_sql_query('select * from cities', con)

        for city_slug in db_config['cities']:
            batch_path = create_files(cities, city_slug, db_config, save_path=Path(save_path))

        self.batch(batch_path)

if __name__ == "__main__":

    Fire(Run)