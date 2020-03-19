CREATE EXTERNAL TABLE IF NOT EXISTS {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }} (
    {% for c in columns %}
        {%- if loop.last -%}
            {{ c }} string
        {%- else -%}
            {{ c }} string,
        {%- endif %} 
        {% endfor %}
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED by '|'
  LOCATION '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}';