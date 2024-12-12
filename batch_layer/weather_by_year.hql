create table haichenfu_weathers_by_year (
    year smallint, clear_count int, fog_count int, rain_count int, snow_count int,
    hail_count int, thunder_count int, tornado_count int, visibility_clean_count int,
    visibility_moderate_count int, visibility_low_count int, visibility_poor_count int,
    temp_very_cold_count int, temp_cold_count int, temp_chilly_count int, temp_warm_count int,
    temp_hot_count int
)
stored as orc;


insert overwrite table haichenfu_weathers_by_year
select w.year as year, 
sum(if(!w.fog and !w.rain and !w.snow and !w.hail and !w.thunder and !w.tornado, 1, 0)) as clear_count,
sum(if(w.fog, 1, 0)) as fog_count,
sum(if(w.rain, 1, 0)) as rain_count,
sum(if(w.snow, 1, 0)) as snow_count,
sum(if(w.hail, 1, 0)) as hail_count,
sum(if(w.thunder, 1, 0)) as thunder_count,
sum(if(w.tornado, 1, 0)) as tornado_count,
sum(if(w.visibility_clean, 1, 0)) as visibility_clean_count,
sum(if(w.visibility_moderate, 1, 0)) as visibility_moderate_count,
sum(if(w.visibility_low, 1, 0)) as visibility_low_count,
sum(if(w.visibility_poor, 1, 0)) as visibility_poor_count,
sum(if(temp_very_cold, 1, 0)) as temp_very_cold_count,
sum(if(temp_cold, 1, 0)) as temp_cold_count,
sum(if(temp_chilly, 1, 0)) as temp_chilly_count,
sum(if(temp_warm, 1, 0)) as temp_warm_count,
sum(if(temp_hot, 1, 0)) as temp_hot_count
from haichenfu_weather w
where w.station = 725340
group by w.year;


select clear_count, fog_count, rain_count, temp_hot_count from haichenfu_weathers_by_year where year = 2024;