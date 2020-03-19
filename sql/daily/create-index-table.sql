create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with ratios as (
	select 
		d.region_slug,
		d."month",
		d.dow,
		d."day",
		d.sum_length as observed,
		h.expected_2019,
		h.expected_2020,
		d.sum_length / h.expected_2019 as ratio_19,
		d.sum_length / h.expected_2020 as ratio_20
	from (
		select
			h20.region_slug,
			h20.dow,
			h19.avg_sum_length as expected_2019,
			h20.avg_sum_length as expected_2020
		from {{ athena_database }}.{{ slug }}_daily_filtered_2019 h19
		join {{ athena_database }}.{{ slug }}_daily_filtered_2020 h20
		on h19.region_slug = h20.region_slug
		and h19.dow = h20.dow) h
	join {{ athena_database }}.{{ slug }}_daily_filtered_daily d
	on d.region_slug = h.region_slug
	and d.dow = h.dow)
select
	metadata.region_slug,
	metadata.region_name,
	metadata.country_name_idb_eng as country_name,
	metadata.country_iso as country_iso_code,
	metadata.idb_code as country_idb_code,
	metadata.region_type,
	metadata.population,
	metadata.timezone,
	ratios.month,
	ratios.day,
	ratios.dow,
	ratios.observed,
	ratios.expected_2019,
	ratios.expected_2020,
	ratios.ratio_19,
	ratios.ratio_20,
	metadata.region_shapefile_wkt
from ratios
join 
	(select 
		r.*,
		c.idb_code,
		c.country_name_idb_eng
	from {{ athena_database }}.{{ slug }}_metadata_country_waze_to_iso c
	join {{ athena_database }}.{{ slug }}_metadata_regions_metadata r
	on c.iso2_code = r.country_iso) metadata
on ratios.region_slug = metadata.region_slug