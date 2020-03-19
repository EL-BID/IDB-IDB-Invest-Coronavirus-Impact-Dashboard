CREATE EXTERNAL TABLE IF NOT EXISTS {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }} (
  `country` string , 
  `country_iso` string,
  `city` string , 
  `timezone` string,
  `year` int,
  `month` int,
  `dow` int , 
  `day` int , 
  `hour` int , 
  `level` int,
  `sum_length` bigint
  )
  PARTITIONED BY (
	city_slug string
  )
  STORED AS ORC
  LOCATION '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}'
	 TBLPROPERTIES (
	  'classification'='orc', 
	  'compressionType'='zlib');