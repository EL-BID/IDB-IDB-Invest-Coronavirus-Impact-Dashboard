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
		expected_2020,
		observed / expected_2020 as ratio_20
	from (	
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
	metadata.timezone,
	case when metadata.timezone is not null then 
			rpad(cast(at_timezone(date_parse(
					concat('2020', '-', cast(month as varchar), '-', cast(day as varchar),
						   ' ', cast(hour as varchar), ':', '00', ':' ,'00'), 
					'%Y-%m-%d %k:%i:%s'),
				metadata.timezone) as varchar), 19, '000') 
		 else null end timestamp_timezone,
	date_parse(concat('2020', '-', cast(month as varchar), '-', cast(day as varchar),
						   ' ', cast(hour as varchar), ':', '00', ':' ,'00'), 
					'%Y-%m-%d %k:%i:%s') timestamp,
	ratios.observed,
	(ratio_20 - 1) * 100 as tci
from ratios
join {{ athena_database }}.{{ slug }}_analysis_metadata_variation metadata
on ratios.region_slug = metadata.region_slug
where daily_approved = true
or metadata.region_slug in ('{{ cv_exception | join(', ') }}')