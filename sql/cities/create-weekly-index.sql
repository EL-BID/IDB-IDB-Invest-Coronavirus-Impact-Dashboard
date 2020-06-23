create table {{ athena_database }}.{{ slug }}_{{ raw_table }}_{{ name }}
with (
      external_location = '{{ s3_path }}/{{ slug }}/{{ current_millis }}/{{ raw_table }}/{{ name }}/',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select 
	d.region_slug,
	d.grid_id,
	d.week_number,
	d.min_month,
	d.min_day,
	d.max_month,
	d.max_day,
	tci,
	expected_2020,
	(tci / expected_2020 - 1) * 100 as tci_perc_change
from (
	select 
		region_slug,
		grid_id,
		sum(expected_2020) expected_2020
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
		group by region_slug, grid_id,  dow) b
	group by region_slug,  grid_id
) h
join (
	select
		region_slug,
		grid_id,
		WEEK(date_parse(concat(cast(year as varchar), ' ', cast(month as varchar), ' ', cast(day as varchar)), '%Y %m %e')) week_number,
		min_by(month, dow) min_month,
		min_by(day, dow) min_day,
		max_by(month, dow) max_month,
		max_by(day, dow) max_day,
		sum(tci) tci
	from {{ athena_database }}.{{ slug }}_{{ raw_table }}_country_cities
	group by region_slug, grid_id,
			WEEK(date_parse(concat(cast(year as varchar), ' ', cast(month as varchar), ' ', cast(day as varchar)), '%Y %m %e'))) d
on d.region_slug = h.region_slug
and d.grid_id = h.grid_id