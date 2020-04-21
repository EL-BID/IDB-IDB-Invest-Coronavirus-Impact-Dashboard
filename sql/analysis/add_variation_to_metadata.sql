create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with coef_var as (
	with weekly as (
		select 
			region_slug,
			avg(sum_length) as weekly_mean,
			stddev_pop(sum_length) as weekly_std,
			stddev_pop(sum_length) / avg(sum_length) weekly_coef_var	
		from (
			select 
				region_slug, 
				sum(sum_length) as sum_length
			from {{ athena_database }}.{{ slug }}_analysis_analysis_daily
			group by region_slug,
					WEEK(date_parse(concat(cast(year as varchar), ' ', cast(month as varchar), ' ', cast(day as varchar)), '%Y %m %e')))
		group by region_slug)
	select 
		d.*,
		w.weekly_mean,
		w.weekly_std,
		w.weekly_coef_var,
		case when d.daily_coef_var <= {{ variation_coef_thershold }} then true else false end daily_approved,
		case when w.weekly_coef_var <= {{ variation_coef_thershold }} then true else false end weekly_approved
	from (
		select region_slug,
			count(*) as n_days,
			avg(sum_length) as daily_mean,
			stddev_pop(sum_length) as daily_std,
			stddev_pop(sum_length) / avg(sum_length) daily_coef_var
		from {{ athena_database }}.{{ slug }}_analysis_analysis_daily
		group by region_slug ) d
	join weekly w
	on d.region_slug = w.region_slug)
select m.*,
	   st_area(m.region_shapefile_binary) area,
	   c.n_days,
	   c.daily_mean,
	   c.daily_std,
	   c.daily_coef_var,
	   c.weekly_mean,
	   c.weekly_std,
	   c.weekly_coef_var,
	   c.daily_approved,
	   c.weekly_approved
from coef_var c
right join {{ athena_database }}.{{ slug }}_metadata_metadata_ready m
on m.region_slug = c.region_slug