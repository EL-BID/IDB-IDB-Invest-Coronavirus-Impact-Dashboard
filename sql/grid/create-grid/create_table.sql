CREATE EXTERNAL TABLE IF NOT EXISTS {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }} (
  `grid_id` string,
  `year` int,
  `month` int,
  `day` int,
  `hour` int,
  `dow` int , 
  `tci` double
  )
  PARTITIONED BY (
    region_slug string
  )
  STORED AS ORC
  LOCATION '{{ s3_path }}/{{ slug }}/tci_grid/{{ region_slug }}/{{ raw_table }}/{{ current_millis }}_{{ name }}'
	 TBLPROPERTIES (
	  'classification'='orc', 
	  'compressionType'='zlib');