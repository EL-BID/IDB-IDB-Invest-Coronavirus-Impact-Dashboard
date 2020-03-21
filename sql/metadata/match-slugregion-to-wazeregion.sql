create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select c.country as country_waze_code, 
	   m.region_slug,
	   c.city as region_waze_name,
	   m.region_shapefile_wkt as region_slug_wkt,
	   c.city_boundary as region_waze_wkt
from {{ athena_database }}.{{ slug }}_historical_cities_boundaries_{{ year }} c
join {{ athena_database }}.{{ slug }}_metadata_metadata_ready m
on	c.country = m.waze_code
and 
	(st_area(st_intersection(c.city_boundary, m.region_shapefile_binary)) / 
    st_area(c.city_boundary)) > {{ polygon_area_minimum }}