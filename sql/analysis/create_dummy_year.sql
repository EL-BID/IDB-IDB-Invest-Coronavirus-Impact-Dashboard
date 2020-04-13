CREATE EXTERNAL TABLE IF NOT EXISTS {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }} (
    `year` int,
    `month` int,
    `day` int,
    `dow` int,
    `sum_length` int
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED by '|'
  LOCATION '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}';