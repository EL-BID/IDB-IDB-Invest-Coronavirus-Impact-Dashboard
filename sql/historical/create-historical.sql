create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with t as (
      select 
      		country,
      		city,
            length,
            year(retrievaltime) as year,
            month(retrievaltime) as month,
            day(retrievaltime) as day,
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
                          city, length,
                          arbitrary(from_unixtime(retrievaltime/1000)) retrievaltime
                from spd_sdv_waze_reprocessed.jams_ready
                where regexp_like(datetime, '{{ historical_2019_dates }}')
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
                                        blockingalertuuid, startnode, endnode))
select
      country country_iso,
      city,
      dow, 
      avg(sum_length) as avg_sum_length
from (
	select 
		  country,
		  city,
	      month, 
	      dow, 
	      day, 
	      sum(length) as sum_length
	from t
	where n_row = 1
	group by  city, country, month, dow, day)
group by country, city, dow