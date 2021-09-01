create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with dates as (
	select distinct 
		year, 
		"month", 
		day, 
		dow,
		date_parse(concat(cast(year as varchar), ' ', 
		cast(month as varchar), ' ', 
		cast(day as varchar)), '%Y %m %e') as date
	from {{ athena_database }}.{{ slug }}_daily_daily
	where date_parse(concat(cast(year as varchar), ' ', 
		cast(month as varchar), ' ', 
		cast(day as varchar)), '%Y %m %e') >= date('2020-03-09')
	),
weeks as (
	select year, month, day,
		--SUM (monday) OVER (ORDER BY date) AS week_number_obs
		FLOOR( ((ROW_NUMBER() over (ORDER BY date)) -1) /7 )+1 AS week_number_obs,
		WEEK(date_parse(concat(cast(year as varchar), ' ', 
		cast(month as varchar), ' ', 
		cast(day as varchar)), '%Y %m %e')) week_number
	from dates
	order by date	
),
ratios as (
	select 
		d.region_slug,
		d.week_number,
		d.week_number_obs,
		d.min_year,
		d.min_month,
		d.min_day,
		d.max_year,
		d.max_month,
		d.max_day,
		d.observed,
		h.expected_2020,
		cast(d.observed as double) / cast(h.expected_2020 as double) as ratio_20
	from (		
	        select
				region_slug,
				sum(expected_2020) expected_2020
			from (
				select
					region_slug,
					dow,
					sum(tci) as expected_2020
				from {{ athena_database }}.{{ slug }}_daily_historical_2020
				group by region_slug, month, day, dow)
            group by region_slug) h
			--group by region_slug,  dow) h
	join (
		select
			region_slug,
			week_number,
            week_number_obs,
			min_by(year, dow) min_year,
			min_by(month, dow) min_month,
			min_by(day, dow) min_day,
            max_by(year, dow) max_year,
			max_by(month, dow) max_month,
			max_by(day, dow) max_day,
			sum(tci) observed
		from (
               select 
                    da.*, 
                    weeks.week_number_obs, 
                    weeks.week_number 
               from {{ athena_database }}.{{ slug }}_daily_daily as da
               join weeks 
               on (da."year" = weeks.year and
                   da."month" = weeks.month and
                   da."day" = weeks.day)
             )
		group by region_slug, 
                week_number_obs,
				week_number
		) d
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
    ratios.week_number_obs,
	ratios.min_year,
	ratios.min_month,
	ratios.min_day,
	ratios.max_year,
	ratios.max_month,
	ratios.max_day,
	ratios.observed,
	ratios.expected_2020,
	ratios.ratio_20,
	(ratio_20 - 1) * 100 as tcp,
	metadata.dashboard,
	metadata.region_shapefile_wkt
from ratios
join {{ athena_database }}.prod_analysis_metadata_variation metadata
on ratios.region_slug = metadata.region_slug
where weekly_approved = true
or metadata.region_slug in ('{{ cv_exception | join(', ') }}')