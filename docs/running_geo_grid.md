
# Split Latin-America in Squares


---

**Project:** Coronavirus Dashboard

**Objective:** Split Latin-America geometry to create a distributed table.


**Storage:** Data is saved as csv  at `shared/spd-sdv-omitnik-waze/corona/geo_partition`

`coarse_id`
`figures`
`geo_id`
`geo_lines`
`lines`


### Steps


1. Download days/weeks of waze jams data.
Get a random sample of several days and put it in memory, should be faster.

```
SELECT distinct datetime
FROM pwazetransformeddb.jams
ORDER BY RANDOM()
LIMIT 100
```
R: Downloaded sample of 50 days. The days were selected from data available from January 15 to December 15 each year from 2019 to 2021. Only week days were considered and random seeds were defined. **Code** can be found at `notebooks/katana_bounds.ipynb` (1.1  Sample dates). 

- Lines 21,904,128
- Jams 507,139,112


<br> 



2. Coarse grid for lines using H3 Grid resolution 1 and resolution 2. 

This is a result of the slow run time of katana grid. So the idea first intersect tiles and then inside the tiles lines. Every grid has one millon or less lines included. 

R: The code can be found at `src/runners/split_polygon.py` by functions `_new_res_coarse_grid()` and `_create_coarse_grid()`. The data can be found at the S3 bucket in `shared/spd-sdv-omitnik-waze/corona/geo_partition/coarse_id/coarse_grid_sample.csv`

<br> 

3. Use this function to create a grid that is proportional to the density of data. I.e. if there is more data, then the tile is smaller. São Paulo should have several tiles and the amazon should have one. Example attached in html.

R: This is created at the Katana function found at `src/runners/split_polygon.py`.

4. Tune the parameters to get a reasonable number of tiles. 

5. Create a support table. The support table has to carry geo partition information.
You have to build this table once with all partitions. After that, the table will remain the same. 
Support table schema:
```
geo_partition_id --> string or int
geometry         --> athena geometry type
geometry_wkt     --> string
```

6. Create a table in athena that has the same columns as the raw table with an extra column: `geo_partition_id`. The query should look like this:
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

7. To update it, just run an INSERT INTO with the missing days/hours

8. Now you have data partitioned by geography. Every time that you need to query for a region, you can use the support table to get the geo_partition_ids. You'd just have to add this snippet on the existing queries.
```
select geo_partition_id
from new_raw_dataset.support_geo_partitions
where ST_INTERSECT(<region_slug>, t2.geometry)
```


9. You have to find out which geo partitions intersect with your region slug through intersection 
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
