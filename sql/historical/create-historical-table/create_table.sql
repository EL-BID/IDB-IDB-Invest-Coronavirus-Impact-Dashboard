CREATE EXTERNAL TABLE IF NOT EXISTS {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }} (
  `dow` int , 
  `avg_sum_length` double
  )
  PARTITIONED BY (
	  region_slug string
  )
  STORED AS ORC
  LOCATION '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}'
	 TBLPROPERTIES (
	  'classification'='orc', 
	  'compressionType'='zlib');