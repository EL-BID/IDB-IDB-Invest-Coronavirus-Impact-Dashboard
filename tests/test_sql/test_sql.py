from src.utils import get_data_from_athena, get_config, connect_athena
import pandas as pd
import pytest

def get_tables(key):

    config = get_config('configs/config-template.yaml')

    return [[config['athena_database'], config['slug'], t] 
            for t in config['tests'][key]]

@pytest.mark.parametrize(
    "athena_database,slug,table",
    get_tables('all_polygons'),
)
def test_all_polygons(athena_database, slug, table):

    con = connect_athena()

    query = f"""
    select region_slug
    from {athena_database}.{slug}_metadata_regions_metadata
    where region_slug not in (
        select region_slug
        from {athena_database}.{slug}_{table}
        group by region_slug)
    group by region_slug
    """

    assert len(pd.read_sql_query(query, con)['region_slug'].to_list()) == 0 

@pytest.mark.parametrize(
    "athena_database,slug,table",
    get_tables('existence'),
)
def test_existence(athena_database, slug, table):

    con = connect_athena()

    query = f"""
    select *
    from {athena_database}.{slug}_{table}
    limit 10
    """ 
    
    pd.read_sql_query(query, con)