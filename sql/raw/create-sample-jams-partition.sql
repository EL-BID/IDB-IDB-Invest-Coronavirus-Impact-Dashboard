create table spd_sdv_waze_corona.raw_sample_jams_partition
with (
      external_location = 's3://iadbprod-public-stata-as-a-service/spd-sdv-omitnik-waze/corona/raw/jams_partition',
	  format='orc', orc_compression = 'ZLIB'
      ) as
with ta as (
	select *
	from spd_sdv_waze_corona.raw_sample_jams 
)	
select ta.*, tb.*,
	st_intersects(
		ST_GeometryFromText(ta.line_wkt),
		ST_GeometryFromText(tb.geo_partition_wkt)
	) geo_intersection
from  ta
CROSS JOIN  spd_sdv_waze_corona.raw_geo_partition tb	
where st_intersects(
		ST_GeometryFromText(ta.line_wkt),
		ST_GeometryFromText(tb.geo_partition_wkt)
	)
