-- copy original raw dataset jams
create table spd_sdv_waze_corona.raw_copy_jams
with (
      external_location = 's3://iadbprod-public-stata-as-a-service/spd-sdv-omitnik-waze/corona/raw/jams',
	  format='orc', orc_compression = 'ZLIB'
      ) as
select *
from pwazetransformeddb.jams
where datetime = '20210701';