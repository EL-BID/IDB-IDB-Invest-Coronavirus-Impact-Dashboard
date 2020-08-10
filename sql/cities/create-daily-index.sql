create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
) as
select 
    d.region_slug,
    d.grid_id,
    d.month,
    d.day,
    d.dow,
    tci,
    expected_2020,
    (tci / expected_2020 - 1) * 100 as tci_perc_change
from (
    select
        region_slug,
        grid_id,
        dow,
        avg(expected_2020) expected_2020
    from (
        select
            region_slug,
            grid_id,
            dow,
            sum(tci) as expected_2020
        from {{ athena_database }}.{{ slug }}_{{ raw_table }}_country_cities_2020
        group by region_slug, grid_id, month, day, dow)
    group by region_slug, grid_id,  dow
) h
join (
    select
        region_slug,
        grid_id,
        month,
        day,
        dow,
        sum(tci) tci
    from {{ athena_database }}.{{ slug }}_{{ raw_table }}_country_cities
    group by region_slug, grid_id,
                month, day, dow) d
on d.region_slug = h.region_slug
and d.grid_id = h.grid_id
and d.dow = h.dow