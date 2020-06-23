insert into {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
select 
	t.*,
	g.id_parent
from (
      select * 
      from {{ athena_database }}.{{ slug }}_{{ from_table }}
      where region_slug = '{{ region_slug }}'
      ) t
join (
      select * 
      from {{ athena_database }}.{{ slug }}_{{ raw_table }}_country_resolutions
      where resolution < 10 
      and region_slug = '{{ region_slug }}'
      and "group" = {{ group }}
      ) g
on st_intersects(
      st_polygon(g.wkt),
      st_line(t.line)) 