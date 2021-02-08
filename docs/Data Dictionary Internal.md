

Database: pwazetransformeddb
Table: jams

| column_name       | Description   | data_type                      |
|:------------------|:--------------|:-------------------------------|
| blockingalertuuid |               | varchar                        |
| city              |               | varchar                        |
| country           |               | varchar                        |
| day               |               | bigint                         |
| delay             |               | bigint                         |
| endnode           |               | varchar                        |
| endtime           |               | varchar                        |
| endtimemillis     |               | bigint                         |
| hour              |               | bigint                         |
| length            |               | bigint                         |
| level             |               | bigint                         |
| line              |               | array(row(x double, y double)) |
| line_wkt          |               | varchar                        |
| month             |               | bigint                         |
| pubmillis         |               | bigint                         |
| retrievaltime     |               | bigint                         |
| roadtype          |               | bigint                         |
| speed             |               | double                         |
| speed_new         |               | double                         |
| speedkmh          |               | double                         |
| speedkmh_new      |               | double                         |
| startnode         |               | varchar                        |
| starttime         |               | varchar                        |
| starttimemillis   |               | bigint                         |
| street            |               | varchar                        |
| turntype          |               | varchar                        |
| type              |               | varchar                        |
| uuid              |               | bigint                         |
| year              |               | bigint                         |
| datetime          |               | varchar                        |




Database: spd_sdv_waze_corona
Table: prod_daily_daily_index

| column_name          | Description   | data_type   |
|:---------------------|:--------------|:------------|
| last_updated_utc     |               | timestamp   |
| region_slug          |               | varchar     |
| region_name          |               | varchar     |
| country_name         |               | varchar     |
| country_iso_code     |               | varchar     |
| country_idb_code     |               | varchar     |
| region_type          |               | varchar     |
| population           |               | varchar     |
| timezone             |               | varchar     |
| year                 |               | integer     |
| month                |               | integer     |
| day                  |               | integer     |
| dow                  |               | integer     |
| observed             |               | bigint      |
| expected_2020        |               | double      |
| ratio_20             |               | double      |
| tci                  |               | double      |
| dashboard            |               | varchar     |
| region_shapefile_wkt |               | varchar     |




Database: spd_sdv_waze_corona
Table: prod_daily_weekly_index

| column_name          | Description   | data_type   |
|:---------------------|:--------------|:------------|
| last_updated_utc     |               | timestamp   |
| region_slug          |               | varchar     |
| region_name          |               | varchar     |
| country_name         |               | varchar     |
| country_iso_code     |               | varchar     |
| country_idb_code     |               | varchar     |
| region_type          |               | varchar     |
| population           |               | varchar     |
| timezone             |               | varchar     |
| week_number          |               | bigint      |
| min_year             |               | integer     |
| min_month            |               | integer     |
| min_day              |               | integer     |
| max_month            |               | integer     |
| max_day              |               | integer     |
| observed             |               | bigint      |
| expected_2020        |               | double      |
| ratio_20             |               | double      |
| tcp                  |               | double      |
| dashboard            |               | varchar     |
| region_shapefile_wkt |               | varchar     |


