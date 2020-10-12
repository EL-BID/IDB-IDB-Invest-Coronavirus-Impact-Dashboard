create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}_{{ p_name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ region_slug }}/{{ raw_table }}/{{ current_millis }}_{{ name }}/{{ p_path }}',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select
 	g.id as grid_id,
	t.year,
	t.month,
	t.day,
	t.hour,
	t.dow,
	cast(sum(tci * ST_LENGTH(st_intersection(st_polygon(g.wkt),
      st_line(t.line))) / ST_LENGTH(st_line(t.line))) as double) tci
from (
	select *
	from {{ athena_database }}.{{ slug }}_{{ raw_table }}_coarse
	where region_slug = '{{ region_slug }}'
	) t
join (
      select * 
      from {{ athena_database }}.{{ slug }}_{{ raw_table }}_resolutions
      where resolution = {{ granular_resolution }}
      and region_slug = '{{ region_slug }}'
	) g
on t.id_parent = g.id_parent
and st_intersects(
      st_polygon(g.wkt),
      st_line(t.line)) 
group by id, 
		 t.region_slug, 
		 year, 
		 month, 
		 day, 
		 hour, 
		 dow
