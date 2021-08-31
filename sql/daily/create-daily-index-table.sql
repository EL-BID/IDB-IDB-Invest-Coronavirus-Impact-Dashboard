create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with ratios as (
    select 
        d.region_slug,
        d."year",
		d."month",
        d.dow,
        d."day",
        observed,
        expected_2020,
        observed / expected_2020 as ratio_20
	from 
		(select
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
			group by region_slug,  dow) h
    join 
		(select 
			region_slug,
			"year",
		    "month",
		    dow,
		    "day",
		    sum(tci) as observed
		from {{ athena_database }}.{{ slug }}_daily_daily
        where date_parse(concat(cast(year as varchar), ' ', 
            cast(month as varchar), ' ', 
            cast(day as varchar)), '%Y %m %e') >= date('2020-03-09')
		group by
		        region_slug,
				"year",
		        "month",
		        dow,
		        "day") d
    on d.region_slug = h.region_slug
    and d.dow = h.dow
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
	ratios.year,
	ratios.month,
	ratios.day,
	ratios.dow,
	ratios.observed,
	ratios.expected_2020,
	ratios.ratio_20,
	(ratio_20 - 1) * 100 as tcp,
	metadata.dashboard,
	metadata.region_shapefile_wkt
from ratios
join {{ athena_database }}.prod_analysis_metadata_variation metadata
on ratios.region_slug = metadata.region_slug
where daily_approved = true
or metadata.region_slug in ('{{ cv_exception | join(', ') }}')
