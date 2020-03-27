create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select uuid, country, 
		city, length, line,
		arbitrary(from_unixtime(retrievaltime/1000)) retrievaltime
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
group by uuid, pubmillis, country, 
		city, street, roadtype, level, length, speed,
		speedkmh, delay, line,  type,  turntype,
		blockingalertuuid, startnode, endnode