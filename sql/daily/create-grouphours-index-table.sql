create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with ratios as (
        select 
            d.region_slug,
            d.month, 
            d.day, 
            d.hour_chunk,
            d.dow,
            observed,
            expected_2019,
            expected_2020,
            observed / expected_2019 as ratio_19,
            observed / expected_2020 as ratio_20
    from (
        select 
            a.region_slug,
            a.hour_chunk,
            a.dow,
            expected_2019,
            expected_2020
        from (
        select 
                region_slug,
                hour_chunk,
                dow,
                avg(expected_2019) expected_2019
            from (
                select
                    region_slug,
                    hour_chunk,
                    dow,
                    sum(tci) as expected_2019
                from ( 
                    select 
                        region_slug,
                        year, month, day,
                        case 
                            when hour between 5 and 9 then '5-9' 
                            when hour between 10 and 15 then '10-15'
                            when hour between 16 and 21 then '16-21'
                            else 'rest'
                        end hour_chunk,
                        dow,
                        tci
                    from {{ athena_database }}.{{ slug }}_daily_historical_2019)
                group by region_slug, year, month, day, hour_chunk, dow)
                group by region_slug, hour_chunk, dow
        ) a
        join (		
            select 
                region_slug,
                hour_chunk,
                dow,
                avg(expected_2020) expected_2020
            from (
                select
                    region_slug,
                    hour_chunk,
                    dow,
                    sum(tci) as expected_2020
                from ( 
                    select 
                        region_slug,
                        year, month, day,
                        case 
                            when hour between 5 and 9 then '5-9' 
                            when hour between 10 and 15 then '10-15'
                            when hour between 16 and 21 then '16-21'
                            else 'rest'
                        end hour_chunk,
                        dow,
                        tci
                    from {{ athena_database }}.{{ slug }}_daily_historical_2020)
                group by region_slug, year, month, day, hour_chunk, dow)
                group by region_slug, hour_chunk, dow
        ) b
        on a.region_slug = b.region_slug
        and a.hour_chunk = b.hour_chunk
        and a.dow = b.dow
    ) h
    join (
        select
        region_slug,
        year, month, day, dow,
        hour_chunk, sum(tci) as observed
        from (
            select 
                region_slug,
                year, month, day, 
                case 
                    when hour between 5 and 9 then '5-9' 
                    when hour between 10 and 15 then '10-15'
                    when hour between 16 and 21 then '16-21'
                    else 'rest'
                end hour_chunk,
                dow,
                tci
            from {{ athena_database }}.{{ slug }}_daily_daily) 
        group by region_slug, year, month, day, hour_chunk, dow) d
    on d.region_slug = h.region_slug
    and h.dow = d.dow
    and h.hour_chunk = d.hour_chunk
)
select
	localtimestamp last_updated_utc,
	metadata.region_slug,
	metadata.timezone,
    month, 
    dow,
    day, 
    hour_chunk,
    expected_2019,
    expected_2020,
    ratio_19,
    ratio_20,
	ratios.observed,
	(ratio_20 - 1) * 100 as tci
from ratios
join {{ athena_database }}.{{ slug }}_analysis_metadata_variation metadata
on ratios.region_slug = metadata.region_slug
where daily_approved = true
or metadata.region_slug in ('{{ cv_exception | join(', ') }}')