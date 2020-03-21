create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select
	r.region_slug,
	d.month,
	d.dow,
	d.day,
	sum(d.sum_length) sum_length
from {{ athena_database }}.{{ slug }}_daily_daily d
join {{ athena_database }}.{{ slug }}_metadata_region_to_waze_2020 r
on d.city = r.region_waze_name
and d.country_waze = r.country_waze_code
group by r.region_slug, d.day, d.month, d.dow