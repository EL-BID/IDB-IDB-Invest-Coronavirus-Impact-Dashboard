CREATE EXTERNAL TABLE IF NOT EXISTS {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }} (
  `region_slug` string,
  `year` int,
  `month` int,
  `dow` int , 
  `day` int,
  `sum_length` double
  )
  PARTITIONED BY (
	  country_iso string
  )
  STORED AS ORC
  LOCATION '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}'
	 TBLPROPERTIES (
	  'classification'='orc', 
	  'compressionType'='zlib');