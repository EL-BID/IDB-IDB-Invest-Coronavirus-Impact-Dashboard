create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}_{{ partition }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/city_slug={{ partition }}',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with t as (
		select 
            level,
            length,
            year(retrievaltime) as year,
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
		  	  city,  level, length,
			  arbitrary(from_unixtime(retrievaltime/1000)) retrievaltime
		from {{ athena_database }}.pipeline_test_daily_daily_raw
		where regexp_like(datetime, '{{ daily_dates }}')
		and st_intersects(
            st_polygon('{{ city_shapefile_wkt }}'),
            st_line(line)) 
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
select 
      '{{ country_name }}' country,
      '{{ country_iso }}' country_iso,
      '{{ city_name }}' city,
      '{{ timezone }}' string,
      year,
      month, 
      dow, 
      day, 
      hour, 
      level,
      sum(length) as sum_length
from t
where n_row = 1
group by level, year, month, dow, day, hour