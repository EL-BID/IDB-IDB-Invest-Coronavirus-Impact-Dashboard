create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with t as (
		select uuid, country, 
		city, street, roadtype, level, length,
		speedkmh, delay, line,
		retrievaltime,
		month(retrievaltime) as month,
		day(retrievaltime) as day,
		hour(retrievaltime) as hour,
		day_of_week(retrievaltime) as dow,
		date_parse(format_datetime(date_add('minute', 
                                    cast(date_diff('minute',
                                          timestamp '{{ reference_timestamp }}', retrievaltime) / {{ feed_frequency }} as bigint) * {{ feed_frequency }},
                                          timestamp '{{ reference_timestamp }}'), 'H:m'), '%H:%i') as time,
		row_number() over (partition by uuid, 
								date_parse(format_datetime(date_add('minute', 
                                    cast(date_diff('minute',
                                          timestamp '{{ reference_timestamp }}', retrievaltime) / {{ feed_frequency }} as bigint) * {{ feed_frequency }},
                                          timestamp '{{ reference_timestamp }}'), 'H:m'), '%H:%i') order by retrievaltime) n_row
	from (
		select uuid, country, 
		  		city, street, roadtype, level, length,
				speedkmh, delay, line, arbitrary(from_unixtime(retrievaltime/1000)) retrievaltime
		from "p-waze-parquet-waze"."jams"
		where regexp_like(datetime, '{{ daily_dates }}')
		and regexp_like(lower(city), '{{ cities }}')
        and
                {% for filter in initial_filters %}
                {%- if loop.last -%}
                    {{ filter }}
                {%- else -%}
                    {{ filter }} and
                {%- endif %} 
                {% endfor %}
		-- to deduplicate
		group by uuid, pubmillis, country, 
			  		city, street, roadtype, level, length, speed,
					speedkmh, delay, line,  type,  turntype,
					blockingalertuuid, startnode, endnode)
)
select city, month, dow, day, hour, sum(length) as sum_length
from t
where n_row = 1
group by city, month, dow, day, hour

