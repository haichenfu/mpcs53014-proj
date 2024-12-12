create external table haichenfu_crimes_by_years_hbase (
    year int, clear_crime bigint, fog_crime bigint, rain_crime bigint, snow_crime bigint,
    hail_crime bigint, thunder_crime bigint, tornado_crime bigint, visibility_clean_crime bigint,
    visibility_moderate_crime bigint, visibility_low_crime bigint, visibility_poor_crime bigint,
    temp_very_cold_crime bigint, temp_cold_crime bigint, temp_chilly_crime bigint, temp_warm_crime bigint,
    temp_hot_crime bigint, 
    clear_count bigint, fog_count bigint, rain_count bigint, snow_count bigint,
    hail_count bigint, thunder_count bigint, tornado_count bigint, visibility_clean_count bigint,
    visibility_moderate_count bigint, visibility_low_count bigint, visibility_poor_count bigint,
    temp_very_cold_count bigint, temp_cold_count bigint, temp_chilly_count bigint, temp_warm_count bigint,
    temp_hot_count bigint
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,crime:clear_crime#b,crime:fog_crime#b,crime:rain_crime#b,crime:snow_crime#b,crime:hail_crime#b,crime:thunder_crime#b,crime:tornado_crime#b,crime:visibility_clean_crime#b,crime:visibility_moderate_crime#b,crime:visibility_low_crime#b,crime:visibility_poor_crime#b,crime:temp_very_cold_crime#b,crime:temp_cold_crime#b,crime:temp_chilly_crime#b,crime:temp_warm_crime#b, crime:temp_hot_crime#b,crime:clear_count#b,crime:fog_count#b,crime:rain_count#b,crime:snow_count#b,crime:hail_count#b,crime:thunder_count#b,crime:tornado_count#b,crime:visibility_clean_count#b,crime:visibility_moderate_count#b,crime:visibility_low_count#b,crime:visibility_poor_count#b,crime:temp_very_cold_count#b,crime:temp_cold_count#b,crime:temp_chilly_count#b,crime:temp_warm_count#b, crime:temp_hot_count#b')
TBLPROPERTIES ('hbase.table.name' = 'haichenfu_crimes_by_years_hbase');

insert overwrite table haichenfu_crimes_by_years_hbase
select year,
clear_crime, fog_crime, rain_crime, snow_crime, 
hail_crime, thunder_crime, tornado_crime,
visibility_clean_crime, visibility_moderate_crime, visibility_low_crime, 
visibility_poor_crime, temp_very_cold_crime, temp_cold_crime, temp_chilly_crime,
temp_warm_crime, temp_hot_crime,
clear_count, fog_count, rain_count, snow_count, 
hail_count, thunder_count, tornado_count,
visibility_clean_count, visibility_moderate_count, visibility_low_count, 
visibility_poor_count, temp_very_cold_count, temp_cold_count, temp_chilly_count,
temp_warm_count, temp_hot_count
from haichenfu_crimes_by_year;