
# Split Latin-America in Squares


---

**Project:** Coronavirus Dashboard

**Objective:** Split Latin-America geometry to create a distributed table by region and date.


**Storage:** Data is saved as csv  at `shared/spd-sdv-omitnik-waze/corona/geo_partition`

- `lines/` sample of unique lines with count of jams, number of appearance in the raw data, in the days sampled. 
- `coarse_id/` coarse grid of lines in hexagons of resolution 1 and 2. 
- `geo_id/` squares, geo_id polygons, result of katana grid algorithm to split data.
- `geo_lines/` lines intersected with geo_id polygons.
- `figures/` frequency tables and maps. 


**Configs:** The configs associated to this step are `config-raw.yaml` and `dependency-graph-raw.yaml`.


<br> 


### Pipeline


**1. Download days/weeks of waze jams data.**

Get a random sample of several days and put it in memory, should be faster.

```
SELECT distinct datetime
FROM pwazetransformeddb.jams
ORDER BY RANDOM()
LIMIT 100
```

*Implementation*. Data was sampled from 50 days. The days were selected from January 15 to December 15 per year from 2019 to 2021. Only week days were considered and random seeds were defined. 

- Lines 21,904,128
- Jams 507,139,112

Code can be found at `notebooks/katana_bounds.ipynb` (1.1  Sample dates) and funtions from `src/runners/split_polygon.py`. The data can be found at the S3 bucket in `shared/spd-sdv-omitnik-waze/corona/geo_partition/lines/line_wkt_count_*.csv`



<br> 




**2. Coarse grid for lines using H3 Grid resolution 1 and resolution 2.**

This step was implemented as consequence of the slow run time of katana grid. So the idea is to first intersect tiles (hexagons) with lines. Every grid has one millon or less lines included. 

*Implementation*. Code can be found in the code at `src/runners/split_polygon.py` function `create_coarse_grid()`. The data can be found at the S3 bucket in `shared/spd-sdv-omitnik-waze/corona/geo_partition/coarse_id/coarse_grid_sample.csv`


<br> 

**3. Katana function to create squares.**

In this step we used the katana method to create a grid that is proportional to the density of data. I.e. if there is more data, then the tile is smaller. São Paulo should have several tiles and the amazon should have one. Example attached in html.

*Implementation*. Code can be found at `src/runners/split_polygon.py` function `create_squares()`. This function creates the katana grid considering the polygon defined for Latin-America. The function `_threshold_density_func()` defines if a square is big enough considering the number of jams in the square proposed represent less than 0.1% of the jams observed in the data sampled in step 1. 

The output is a table with the polygon per square and can be found at the S3 bucket in `shared/spd-sdv-omitnik-waze/corona/geo_partition/geo_id/{cm}/geo_grid_area_{cm}.csv`

A second grid was created in polygons with a number of jams twice or more over the  0.1% of the jams in the data. The code for this step can ve found in function `redo_squares()` at `src/runners/split_polygon.py`. 

For the second grid, the output are several tables, one per polygon, including the polygon of the squares redone. It also can be found at the S3 bucket in `shared/spd-sdv-omitnik-waze/corona/geo_partition/geo_id/{cm}/geo_grid_area_{cm}POLYGON ((*)).csv`

1. `2021083013081630344071/`: 364 squares
2. `2021091413091631639640/`: 10 squares 
2. `2021092622091632708970/`: 10 squares 
3. `2021092812091632847410/`: 6 squares 
4. `2021100101101633066162/`: 70 squares 
4. `2021100323101633318356/`: 80 squares (FINAL VERSION)


**3b.** Tune the parameters to get a reasonable number of tiles. 

**Implementation**. Parameters in this step are defined at the function `_katana_grid()`. 

<br> 

**4. Frequency of lines per square**

In this step first we assign a square or geo_id to each line in the data.

**Implementation**. Code can be found at `src/runners/split_polygon.py` function `density_squares()`. The output is a csv table with each line observed assigned to a geo_id. 


**4b.** Distribution table

**Implementation**. Code can be found at `src/runners/split_polygon.py` function `_get_dist_table()`. The output is a csv table with the number of unique lines and jams per geo_id. 


**4c.** Frequency and distribution table for squares redone


<br> 

**5. Create a support table. The support table has to carry geo partition information.**

You have to build this table once with all partitions. After that, the table will remain the same. 
Support table schema:

```
geo_partition_id --> string or int
geometry         --> athena geometry type
geometry_wkt     --> string
```

<br> 

**6. Create a table in athena that has the same columns as the raw table with an extra column: `geo_partition_id`. **

The query should look like this:
That might not work for all full dataset, so you can first create an empty table, then run this query daily using INSERT INTO to add the new data to the final table.

```
create table {{dataset_name}}.jams
with (
external_location = '{{ s3_path }}/{{ some_location }}/',
format='orc', orc_compression = 'ZLIB', partition_by = ['datetime', 'geo_partition_id']
) as
select t1.*, t2.geo_partition_id
from original_raw_dataset.jams t1 join new_raw_dataset.support_geo_partitions t2
on ST_INTERSECT(t1.line, t2.geometry)
```

<br> 

**7. To update it, just run an INSERT INTO with the missing days/hours**

<br> 

**8. Now you have data partitioned by geography.**

Every time that you need to query for a region, you can use the support table to get the geo_partition_ids. You'd just have to add this snippet on the existing queries.

```
select geo_partition_id
from new_raw_dataset.support_geo_partitions
where ST_INTERSECT(<region_slug>, t2.geometry)
```

<br> 

**9.** You have to find out which geo partitions intersect with your region slug through intersection 
Example of how a query should look like

```
SELECT
*
FROM raw_jams_data_partitioned_by_geo
WHERE geo_partition_id IN (
SELECT geo_partition_id

FROM support_table
WHERE st_intersects(geometry,
st_polygon(<REGION_SLUG_GEO>))
```


### Run 

To run the pipeline use the following command. 

```
python src/entrypoint.py single --slug=raw --n_tries=1 --dependency_graph_path=configs/dependency-graph-raw.yaml --config_path=configs/config-raw.yaml &>> log.log
```

The slug to run this pipeline is `raw`. Remember that each slug has a different directory in S3 where data is stored. Also, the slug defines the name of the table name created in Athena.

----

**Reference:**

- Geometric objects tutorial: https://autogis-site.readthedocs.io/en/latest/notebooks/L1/geometric-objects.html
- Split methodology: https://snorfalorpagus.net/blog/2016/03/13/splitting-large-polygons-for-faster-intersections/
- Running example: https://gist.github.com/JoaoCarabetta/8a5df60ac0012e56219a5b2dffcb20e3
- Katana function: https://github.com/JoaoCarabetta/osmpy/blob/36e0b75bd65c6d8d6d9d379c410561ba1ea19bbf/osmpy/core.py#L46
- Katana example: https://gist.github.com/JoaoCarabetta/8a5df60ac0012e56219a5b2dffcb20e3
- Random sample in Athena: https://stackoverflow.com/questions/44510714/random-sample-of-size-n-in-athena
- Athena partition projection: https://docs.aws.amazon.com/athena/latest/ug/partition-projection-kinesis-firehose-example.html
- Polygons and geojson visualization: https://geojson.io/
