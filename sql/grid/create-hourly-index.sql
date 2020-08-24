create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format = 'textfile', 
      field_delimiter='|',
	  partitioned_by  = ARRAY['region_slug', 'yearmonth']
      ) as
with ratios as (
	select 
		grid_id,
		cast(year as varchar) year,
		cast(month as varchar) month, 
		cast(day as varchar) day, 
		cast(hour as varchar) hour,
		dow,
		tci,
		region_slug
	from {{ athena_database }}.{{ slug }}_{{ raw_table }}_grid
	)
select
	localtimestamp last_updated_utc,
	metadata.timezone,
	case when metadata.timezone is not null then 
			rpad(cast(at_timezone(date_parse(
					concat(year, '-', month, '-', day ,
						   ' ', hour, ':', '00', ':' ,'00'), 
					'%Y-%m-%d %k:%i:%s'),
				metadata.timezone) as varchar), 19, '000') 
		 else null end timestamp_local,
	date_parse(concat(year, '-', month, '-', day,
						   ' ', hour , ':', '00', ':' ,'00'), 
					'%Y-%m-%d %k:%i:%s') timestamp,
	ratios.*,
	concat(year, month) as yearmonth
from ratios
join {{ athena_database }}.{{ slug }}_analysis_metadata_variation metadata
on ratios.region_slug = metadata.region_slug