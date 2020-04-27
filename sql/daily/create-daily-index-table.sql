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
		from {{ athena_database }}.{{ slug }}_daily_historical_2019 h19
		join {{ athena_database }}.{{ slug }}_daily_historical_2020 h20
		on h19.region_slug = h20.region_slug
		and h19.dow = h20.dow) h
	join {{ athena_database }}.{{ slug }}_daily_daily d
	on d.region_slug = h.region_slug
	and d.dow = h.dow)
select
	localtimestamp last_updated_utc,
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
	(ratio_20 - 1) * 100 as tcp,
	metadata.dashboard,
	metadata.region_shapefile_wkt
from ratios
join {{ athena_database }}.{{ slug }}_analysis_metadata_variation metadata
on ratios.region_slug = metadata.region_slug
where daily_approved = true
or metadata.region_slug in ('{{ cv_exception | join(', ') }}')
