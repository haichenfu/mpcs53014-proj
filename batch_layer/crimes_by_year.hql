create table haichenfu_crimes_by_year (
year int, clear_crime int, fog_crime int, rain_crime int, snow_crime int,
    hail_crime int, thunder_crime int, tornado_crime int, visibility_clean_crime int,
    visibility_moderate_crime int, visibility_low_crime int, visibility_poor_crime int,
    temp_very_cold_crime int, temp_cold_crime int, temp_chilly_crime int, temp_warm_crime int,
    temp_hot_crime int, 
    clear_count int, fog_count int, rain_count int, snow_count int,
    hail_count int, thunder_count int, tornado_count int, visibility_clean_count int,
    visibility_moderate_count int, visibility_low_count int, visibility_poor_count int,
    temp_very_cold_count int, temp_cold_count int, temp_chilly_count int, temp_warm_count int,
    temp_hot_count int
)
stored as orc;

insert overwrite table haichenfu_crimes_by_year
select w.year as year, 
sum(if(!w.fog and !w.rain and !w.snow and !w.hail and !w.thunder and !w.tornado, 1, 0)) as clear_crime,
sum(if(w.fog, 1, 0)) as fog_crime,
sum(if(w.rain, 1, 0)) as rain_crime,
sum(if(w.snow, 1, 0)) as snow_crime,
sum(if(w.hail, 1, 0)) as hail_crime,
sum(if(w.thunder, 1, 0)) as thunder_crime,
sum(if(w.tornado, 1, 0)) as tornado_crime,
sum(if(w.visibility_clean, 1, 0)) as visibility_clean_crime,
sum(if(w.visibility_moderate, 1, 0)) as visibility_moderate_crime,
sum(if(w.visibility_low, 1, 0)) as visibility_low_crime,
sum(if(w.visibility_poor, 1, 0)) as visibility_poor_crime,
sum(if(w.temp_very_cold, 1, 0)) as temp_very_cold_crime,
sum(if(w.temp_cold, 1, 0)) as temp_cold_crime,
sum(if(w.temp_chilly, 1, 0)) as temp_chilly_crime,
sum(if(w.temp_warm, 1, 0)) as temp_warm_crime,
sum(if(w.temp_hot, 1, 0)) as temp_hot_crime,
y.clear_count as clear_count,
y.fog_count as fog_count,
y.rain_count as rain_count,
y.snow_count as snow_count,
y.hail_count as hail_count,
y.thunder_count as thunder_count,
y.tornado_count as tornado_count,
y.visibility_clean_count as visibility_clean_count,
y.visibility_moderate_count as visibility_clean_count,
y.visibility_low_count as visibility_low_count,
y.visibility_poor_count as visibility_poor_count,
y.temp_very_cold_count as temp_very_cold_count,
y.temp_cold_count as temp_cold_count,
y.temp_chilly_count as temp_chilly_count,
y.temp_warm_count as temp_warm_count,
y.temp_hot_count as temp_hot_count
from haichenfu_crimes_and_weathers w
join haichenfu_weathers_by_year y 
on w.year = y.year
group by w.year,
    y.clear_count,
    y.fog_count, 
    y.rain_count, 
    y.snow_count, 
    y.hail_count, 
    y.thunder_count, 
    y.tornado_count,
    y.visibility_clean_count, 
    y.visibility_moderate_count, 
    y.visibility_low_count, 
    y.visibility_poor_count, 
    y.temp_very_cold_count, 
    y.temp_cold_count, 
    y.temp_chilly_count, 
    y.temp_warm_count, 
    y.temp_hot_count;

-- testing command to view the table
select year, clear_crime, clear_count, fog_count, fog_crime, rain_count, rain_crime from haichenfu_crimes_by_year where year = 2024; 

