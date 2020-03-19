create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select
	r.region_slug,
	h.dow,
	sum(h.avg_sum_length) avg_sum_length
from {{ athena_database }}.{{ slug }}_historical_{{ table }} h
join {{ athena_database }}.{{ slug }}_metadata_regions_waze_to_slug r
on h.city = r.region_waze_name
and h.country_waze = r.country_waze_code
group by r.region_slug, h.dow