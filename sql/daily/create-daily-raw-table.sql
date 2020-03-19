create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select *
from "p-waze-parquet-waze"."jams"
where regexp_like(datetime, '{{ daily_dates }}')
and
		{% for filter in initial_filters %}
		{%- if loop.last -%}
			{{ filter }}
		{%- else -%}
			{{ filter }} and
		{%- endif %} 
		{% endfor %}
