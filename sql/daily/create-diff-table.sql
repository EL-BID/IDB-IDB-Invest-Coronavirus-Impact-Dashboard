create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select
	d.city,
	d."month",
	d.dow,
	d.day,
	d."hour",
	d.sum_length as day_length,
	h.avg_length as avg_length,
	(d.sum_length / h.avg_length - 1) as diff
from {{ athena_database }}.{{ slug }}_daily_daily d
join {{ athena_database }}.{{ slug }}_historical_historical h
on d.city = h.city 
and d.dow = h.dow
and d."hour" = h."hour"
and d."month" = h."month"
order by city, month, day, hour