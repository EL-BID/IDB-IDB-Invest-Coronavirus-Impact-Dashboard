

### jams

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





### prod_daily_daily

Database: spd_sdv_waze_corona

Table: prod_daily_daily

| column_name   | Description   | data_type   |
|:--------------|:--------------|:------------|
| year          |               | integer     |
| month         |               | integer     |
| day           |               | integer     |
| hour          |               | integer     |
| dow           |               | integer     |
| line          |               | varchar     |
| tci           |               | bigint      |
| region_slug   |               | varchar     |



