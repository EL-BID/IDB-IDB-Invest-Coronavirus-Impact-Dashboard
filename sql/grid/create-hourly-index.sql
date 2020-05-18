create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with ratios as (
	select 
		d.region_slug,
		d.grid_id,
		d.month, 
		d.day, 
		d.hour,
		d.dow,
		tci,
		expected_2020,
		100 * (tci / expected_2020 - 1) as tci_perc_change 
	from (
		select
			region_slug,
			grid_id,
			hour,
			dow,
			avg(tci) expected_2020
		from {{ athena_database }}.{{ slug }}_grid_grid_2020
		group by region_slug, grid_id, hour, dow
		) h
	join {{ athena_database }}.{{ slug }}_grid_grid d
	on d.region_slug = h.region_slug
	and d.grid_id = h.grid_id
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
		 else null end timestamp_local,
	date_parse(concat('2020', '-', cast(month as varchar), '-', cast(day as varchar),
						   ' ', cast(hour as varchar), ':', '00', ':' ,'00'), 
					'%Y-%m-%d %k:%i:%s') timestamp,
	ratios.tci,
	expected_2020,
	tci_perc_change
from ratios
join {{ athena_database }}.{{ slug }}_analysis_metadata_variation metadata
on ratios.region_slug = metadata.region_slug