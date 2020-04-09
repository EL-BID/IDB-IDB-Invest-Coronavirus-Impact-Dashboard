create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select 
	r.*,
	c.idb_code,
	c.waze_code,
	c.country_name_idb_eng,
    ST_POLYGON(replace(r.region_shapefile_wkt, '"', '')) region_shapefile_binary
from {{ athena_database }}.{{ slug }}_metadata_country_waze_to_iso c
join {{ athena_database }}.{{ slug }}_metadata_regions_metadata r
on c.iso2_code = r.country_iso