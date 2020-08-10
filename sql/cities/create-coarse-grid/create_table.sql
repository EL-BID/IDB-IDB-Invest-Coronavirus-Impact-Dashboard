CREATE EXTERNAL TABLE IF NOT EXISTS {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }} (
  `year` int,
  `month` int,
  `day` int,
  `hour` int,
  `dow` int , 
  `line` string,
  `tci` double
  )
  PARTITIONED BY (
    region_slug string,
    id_parent string
  )
  STORED AS ORC
  LOCATION '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}'
	 TBLPROPERTIES (
	  'classification'='orc', 
	  'compressionType'='zlib');