create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with ratios as (
	select 
		d.region_slug,
		d.week_number,
		d.min_year,
		d.min_month,
		d.min_day,
		d.max_month,
		d.max_day,
		observed,
		expected_2020,
		observed / expected_2020 as ratio_20
	from (		
	        select
				region_slug,
				dow,
				avg(expected_2020) expected_2020
			from (
				select
					region_slug,
					dow,
					sum(tci) as expected_2020
				from {{ athena_database }}.{{ slug }}_daily_historical_2020
				group by region_slug, month, day, dow)
            --group by region_slug) h
			group by region_slug,  dow) h
	join (
		select
			region_slug,
			WEEK(date_parse(concat(cast(year as varchar), ' ', cast(month as varchar), ' ', cast(day as varchar)), '%Y %m %e')) week_number,
			min_by(year, dow) min_year,
			min_by(month, dow) min_month,
			min_by(day, dow) min_day,
			max_by(month, dow) max_month,
			max_by(day, dow) max_day,
			sum(tci) observed
		from {{ athena_database }}.{{ slug }}_daily_daily
		group by region_slug, 
				WEEK(date_parse(concat(cast(year as varchar), ' ', cast(month as varchar), ' ', cast(day as varchar)), '%Y %m %e'))) d
	on d.region_slug = h.region_slug)
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
	ratios.week_number,
	ratios.min_year,
	ratios.min_month,
	ratios.min_day,
	ratios.max_month,
	ratios.max_day,
	ratios.observed,
	ratios.expected_2020,
	ratios.ratio_20,
	(ratio_20 - 1) * 100 as tcp,
	metadata.dashboard,
	metadata.region_shapefile_wkt
from ratios
join {{ athena_database }}.{{ slug }}_analysis_metadata_variation metadata
on ratios.region_slug = metadata.region_slug
where weekly_approved = true
or metadata.region_slug in ('{{ cv_exception | join(', ') }}')