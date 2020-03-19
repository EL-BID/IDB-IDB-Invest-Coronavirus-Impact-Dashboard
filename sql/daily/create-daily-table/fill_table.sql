create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}_{{ partition }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/country_waze={{ partition }}',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with t as (
      select 
            city,
            length,
            year(retrievaltime) as year,
            month(retrievaltime) as month,
            day(retrievaltime) as day,
            day_of_week(retrievaltime) as dow,
			date_parse(format_datetime(date_add('minute', 
										cast(date_diff('minute',
											timestamp '{{ reference_timestamp }}', retrievaltime) / {{ feed_frequency }} as bigint) * {{ feed_frequency }},
											timestamp '{{ reference_timestamp }}'), 'H:m'), '%H:%i') as time,
			row_number() over (partition by uuid, 
									date_parse(format_datetime(date_add('minute', 
										cast(date_diff('minute',
											timestamp '{{ reference_timestamp }}', retrievaltime) / {{ feed_frequency }} as bigint) * {{ feed_frequency }},
                                          timestamp '{{ reference_timestamp }}'), 'H:m'), '%H:%i') order by retrievaltime) n_row
      from {{ athena_database }}.pipeline_test_daily_daily_raw
      where country = '{{ partition }}')
select 
	city,
	year,
	month, 
	dow, 
	day, 
	sum(length) as sum_length
from t
where n_row = 1
group by  city, year, month, dow, day

