create table spd_sdv_waze_corona.raw_metadata_partition_ready
with (
      external_location = 's3://iadbprod-public-stata-as-a-service/spd-sdv-omitnik-waze/corona/raw/metadata',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select ta.*, tb.*,
	st_intersects(
		ST_GeometryFromText(ta.geo_partition_wkt),
		ST_GeometryFromText(tb.region_shapefile_wkt)
	) geo_intersection
from spd_sdv_waze_corona.raw_geo_partition ta
CROSS JOIN  spd_sdv_waze_corona.prod_metadata_metadata_ready tb
where st_intersects(
		ST_GeometryFromText(ta.geo_partition_wkt),
		ST_GeometryFromText(tb.region_shapefile_wkt)
	) 