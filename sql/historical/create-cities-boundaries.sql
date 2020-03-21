create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with b as (
	with a as (
		select 	array_distinct(
				transform(
					split(
						replace(
							replace(
								line,
								'LINESTRING('),
						')'), 
					','),
				x -> transform(split(ltrim(x), ' '), y -> cast(y as double)))) as line,
				city,
				country
		from {{ athena_database }}.{{ slug }}_{{ raw_table }}_historical_{{ year }}_raw)
	select city, country, 
			cast(max(point[1]) as varchar) max_lon, 
			cast(min(point[1]) as varchar) min_lon, 
			cast(max(point[2]) as varchar) max_lat, 
			cast(min(point[2]) as varchar) min_lat
	from (a
	cross join unnest("line") t (point))
	group by city, country)
select city, country, 
		concat('polygon((', 
						max_lon, ' ', max_lat, ', ', 
						max_lon, ' ', min_lat, ', ', 
						min_lon, ' ', min_lat, ', ',
						min_lon, ' ', max_lat, ', ',
						max_lon, ' ', max_lat,
				'))') city_boundary
from b