create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}_{{ partition }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/region_slug={{ partition }}',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select 
	year,
	month, 
	dow, 
	day, 
	sum(tci) as tci
from {{ athena_database }}.{{ slug }}_daily_daily_filtered
where n_row = 1
group by  year, month, dow, day

