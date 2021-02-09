### Daily
| Column Name          | Description                                                        |  Type     |
|----------------------|--------------------------------------------------------------------|-----------|
| last_updated_utc     | Last updated date in UTC time                                      | timestamp |
| region_slug          | Region unique name                                                 | string    |
| region_name          | Region human readable name                                         | string    |
| country_name         | Country name in english                                            | string    |
| country_iso_code     | Country code in ISO-2 standard                                     | string    |
| country_idb_code     | Country code in IDB standards                                      | string    |
| region_type          | Region type, e.g. city, country                                    | string    |
| population           | Population of the region                                           | int       |
| timezone             | Timezone of the region                                             | string    |
| year                 | Year                                                               | int       |
| month                | Month                                                              | int       |
| day                  | Day                                                                | int       |
| dow                  | Day of the week                                                    | int       |
| ratio_20             | Percentage change in Traffic Congestion Intensity (TCI) is `change_TCI = (ratio_20 - 1) * 100` | float    |
| tcp                  | Percentage change in Traffic Congestion Intensity (TCI)            | float    |

### Weekly
| Column Name          | Description                                                        |  Type     |
|----------------------|--------------------------------------------------------------------|-----------|
| last_updated_utc     | Last updated date in UTC time                                      | timestamp |
| region_slug          | Region unique name                                                 | string    |
| region_name          | Region human readable name                                         | string    |
| country_name         | Country name in english                                            | string    |
| country_iso_code     | Country code in ISO-2 standard                                     | string    |
| country_idb_code     | Country code in IDB standards                                      | string    |
| region_type          | Region type, e.g. city, country                                    | string    |
| population           | Population of the region                                           | int       |
| timezone             | Timezone of the region                                             | string    |
| week_number          | Week number in 2020                                                | int       |
| min_year             | Year of the week                                                   | int       |
| min_month            | First day of the week month                                        | int       |
| min_day              | First day of the week day                                          | int       |
| max_month            | Last day of the week month                                         | int       |
| min_day              | Last day of the week day                                           | int       |
| ratio_20             | Percentage change in Traffic Congestion Intensity (TCI) is `change_TCI = (ratio_20 - 1) * 100` | float    |
| tcp                  | Percentage change in Traffic Congestion Intensity (TCI)            | float    |

### Metadata
| Column Name          | Description                                                        |  Type     |
|----------------------|--------------------------------------------------------------------|-----------|
| country_name         | Country name in english                                            | string    |
| country_iso          | Country code in ISO-2 standard                                     | string    |
| region_slug          | Region unique name                                                 | string    |
| region_name          | Region human readable name                                         | string    |
| region_type          | Region type, e.g. city, country                                    | string    |
| population           | Population of the region                                           | int       |
| timezone             | Timezone of the region                                             | string    |
| osm_length           | Open Street Maps road length                                       | float     |
| daily_coef_var       | Coeficient of variation for daily TCI in 2019                      | float     |
| weekly_coef_var      | Coeficient of variation for weekly TCI in 2019                     | float     |
| daily_approved       | Whether the data had Daily CV < 0.5 and daily length ratio > 0.2   | bool      |
| weekly_approved      | Whether the data had Weekly CV < 0.5 and daily length ratio > 0.2  | bool      |
																																																																																		
