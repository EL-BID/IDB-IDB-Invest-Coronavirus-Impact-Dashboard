create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with ratios as (
	select 
		d.region_slug,
		d.month, 
		d.day, 
		d.hour,
		d.dow,
		observed,
		expected_2019,
		expected_2020,
		observed / expected_2019 as ratio_19,
		observed / expected_2020 as ratio_20
	from (
		select
			a.region_slug, 
			a.hour,
			a.dow,
			expected_2019,
			expected_2020
		from (
			select 
				region_slug,
				hour,
				dow,
				avg(expected_2019) expected_2019
			from (
				select
					region_slug,
					hour,
					dow,
					sum(tci) as expected_2019
				from {{ athena_database }}.{{ slug }}_daily_historical_2019
				group by region_slug, year, month, day, hour, dow)
				group by region_slug, hour, dow
				) a
		join (		
			select 
				region_slug,
				hour,
				dow,
				avg(expected_2020) expected_2020
			from (
				select
					region_slug,
					hour,
					dow,
					sum(tci) as expected_2020
				from {{ athena_database }}.{{ slug }}_daily_historical_2020
				group by region_slug, year, month, day, hour, dow)
				group by region_slug, hour, dow
			) b
		on a.region_slug = b.region_slug
		and a.hour = b.hour
		and a.dow = b.dow
		) h
	join (
		select
			region_slug,
			month, day, hour, dow,
			sum(tci) observed
		from {{ athena_database }}.{{ slug }}_daily_daily
		group by region_slug, month, day, hour, dow) d
	on d.region_slug = h.region_slug
	and h.dow = d.dow
	and h.hour = d.hour
	)
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
	ratios.dow,
	ratios.month,
	ratios.day,
	ratios.hour,
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