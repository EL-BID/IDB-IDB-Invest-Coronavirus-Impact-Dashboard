create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select cast(uuid as varchar) uuid,
		 country, 
		city, length, line,
		from_unixtime(retrievaltime/1000) retrievaltime
from spd_sdv_waze_reprocessed.jams_ready
where regexp_like(datetime, '20200308|20200309|20200310|20200311|20200312|20200313|20200314|20200315')
and
		{% for filter in initial_filters %}
		{%- if loop.last -%}
			{{ filter }}
		{%- else -%}
			{{ filter }} and
		{%- endif %} 
		{% endfor %}
union all
select cast(uuid as varchar) uuid,
		country, 
		city, length, line,
		from_unixtime(retrievaltime/1000) retrievaltime
from "p-waze-parquet-waze"."jams"
where regexp_like(datetime, '{{ dates }}')
and
		{% for filter in initial_filters %}
		{%- if loop.last -%}
			{{ filter }}
		{%- else -%}
			{{ filter }} and
		{%- endif %} 
		{% endfor %}