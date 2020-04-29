# Calculate TCI Percentage Variation with BigQuery

This is a short tutorial on how to reproduce the TCI Percentage Variation with BigQuery.

:warning: **Unfortunatelly, we couldn't run the queries in BigQuery. The queries are probrably right, but small errors might happen. Please, submit an issue or pull request if you find an error. Or, if you have a suggestion to make this tutorial easier to follow.**

Before you start, please select a region. Usually, you just have access of
Waze data of your city/region. But, this index works better in areas with more traffic. 

If you need to create a polygon, you can use [this website](https://arthur-e.github.io/Wicket/sandbox-gmaps3.html).
 
> Remember, it has to be in a WKT format, like
>
> `POLYGON ((-81.69 40.90,-81.66 40.15,-80.59 40.17,-81.69 40.90))`
>
> and, dates have to be formated as
> 
> `YYYY-MM-DD :: <year>-<month>-<day> :: 2020-05-24`

**Steps**:
1. Create a table/view for TCI current
2. Select a resoanable baseline interval
3. Create a table/view for TCI baseline
4. Create TCI Index
5. Make sure that your region has enough Waze users to produce reliable data.

### TCI Current

The date interval here is the one that you are going to analyse. Make sure that
it does not have any intersection with the baseline date interval.

> TIP: The `<final_date>` can be the most recent day, `CURRENT_DATE()`.


**Create table or view as `tci_current`**.

```sql
SELECT​
    sum(length) as tci_current,​
    date(ts) as date,​
    EXTRACT(DAYOFWEEK from ts) as day_of_week
FROM `[project].[data_set].jams`
WHERE ​
    ST_INTERSECTS(geo, ST_GEOGFROMTEXT(“<YOUR POLYGON>”) IS TRUE
AND​
    ts between DATE <initial_date> and DATE <final_date> 
AND 
    level != 5
GROUP BY​
    date(ts)
```


### TCI Baseline

Select a baseline date interval that represents the most typical week of your region.
A week with **no** holydays, protests, storms, etc.

The dates have to be formated as

`YYYY-MM-DD :: <year>-<month>-<day> :: 2020-05-24`

**Create table or view as `tci_baseline`**.

```sql
SELECT​
    sum(length) as tci_baseline,​
    EXTRACT(DAYOFWEEK from ts) as day_of_week
FROM `[project].[data_set].jams`
WHERE ​
    ST_INTERSECTS(geo, ST_GEOGFROMTEXT(“<YOUR POLYGON>”) IS TRUE
AND​
    ts between DATE <initial_baseline_date> and DATE <initial_baseline_date> 
AND 
    level != 5
GROUP BY​
    EXTRACT(DAYOFWEEK from ts)
```

### TCI Index

The final query just merges both tables creating the TCI Index.

You can optionally add the region name in your final table.

**Create table or view as `tci_index`**.

```sql
SELECT​
    '<region_name>' as region_name
    date​,
    (tci_current / tci_baseline - 1) * 100 as tci_index
FROM `[project].[data_set].tci_baseline` ​
JOIN `[project].[data_set].tci_current` ​
ON  day_of_week
```

## Is your waze data reliable for this region?

### Calculate TCI statistics for historical period

At the IDB, we use the full year of 2019 as our historical reference period.

In our case, the variables are :
- `initial_date_historical = '2019-01-01'`
- `final_date_historical = '2019-12-31'`

```sql
with tci as (
    SELECT​
        sum(length) as tci,​
        date(ts) as date,​
        EXTRACT(DAYOFWEEK from ts) as day_of_week
    FROM `[project].[data_set].jams`
    WHERE ​
        ST_INTERSECTS(geo, ST_GEOGFROMTEXT(“<YOUR POLYGON>”) IS TRUE
    AND​
        ts between DATE <initial_date_historical> and DATE <final_date_historical> 
    AND 
        level != 5
    GROUP BY​
        date(ts))
SELECT
    AVG(tci.tci) as avg_tci,
    STDDEV_POP(tci.tci) as std_tci,
    STDDEV_POP(tci.tci) / AVG(tci.tci) as coef_variation_tci
FROM tci
```


##### Coeficient of Variation Rule

We recomend a coeficient of variation smaller than 0.5, i.e. `coef_variation_tci < 0.5`.

##### Coverage compared to Open Street Maps (OSM)

We recomend the ratio between the average TCI and OSM length to be higher than 0.1.

`avg_tci / total_osm_length > 0.1` 

You can use the following python code to get the OSM road network length for your region.

First, install this package `pip install osm-road-length`. Then, run

```python
import osm_road_length
from shapely import wkt

geometry = wkt.loads('<YOUR POLYGON>')

length = osm_road_length.get(geometry)

accepted_keys = [
    'motorway',
    'trunk',
    'primary',
    'secondary',
    'tertiary',
    'unclassified',
    'residential'
]

total_osm_length = length[length.index.isin(accepted_keys)].sum()
```
