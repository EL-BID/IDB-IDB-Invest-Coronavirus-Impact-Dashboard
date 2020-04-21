create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}_{{ partition }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/country_iso={{ country_iso }}/{{ partition }}',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with daily as (
	with t as (
		select 
				length,
				year(from_unixtime(retrievaltime/1000)) as year,
				month(from_unixtime(retrievaltime/1000)) as month,
				day(from_unixtime(retrievaltime/1000)) as day,
				day_of_week(from_unixtime(retrievaltime/1000)) as dow,
				date_parse(format_datetime(date_add('minute', 
											cast(date_diff('minute',
												timestamp '{{ reference_timestamp }}', from_unixtime(retrievaltime/1000)) / {{ feed_frequency }} as bigint) * {{ feed_frequency }},
												timestamp '{{ reference_timestamp }}'), 'H:m'), '%H:%i') as time,
				row_number() over (partition by uuid, 
										date_parse(format_datetime(date_add('minute', 
											cast(date_diff('minute',
												timestamp '{{ reference_timestamp }}', from_unixtime(retrievaltime/1000)) / {{ feed_frequency }} as bigint) * {{ feed_frequency }},
											timestamp '{{ reference_timestamp }}'), 'H:m'), '%H:%i') order by from_unixtime(retrievaltime/1000)) n_row
		from spd_sdv_waze_reprocessed.jams_ready
		where 
		year(from_unixtime(retrievaltime/1000)) = 2019 and
		{% if waze_code != 'continent' %}   
				country = '{{ waze_code }}' and
			st_intersects(
			st_polygon('{{ region_shapefile_wkt | replace('"', '') }}'),
			st_line(line))
		{%endif %}
			)
	select 
		year,
		month, 
		dow, 
		day, 
		sum(length) as sum_length
	from t
	where n_row = 1
	group by  year, month, dow, day)
select '{{ region_slug }}' region_slug, 
		b.year, b."month", b.dow,  b."day", 
		case when a.sum_length is null then 0 else a.sum_length end sum_length
from {{ athena_database }}.{{ slug }}_analysis_dummy_2019 b
full outer join daily a
on a.year = b."year"
and a."month" = b."month"
and a."day" = b."day"