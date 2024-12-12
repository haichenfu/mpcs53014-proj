create table haichenfu_crimes_and_weathers (
    year smallint, month tinyint, day tinyint, primary_type string, 
    mean_temperature double, mean_visibility double, mean_windspeed double,
  fog boolean, rain boolean, snow boolean, hail boolean, thunder boolean, tornado boolean,
  visibility_clean boolean, visibility_moderate boolean, visibility_low boolean, visibility_poor boolean,
  temp_very_cold boolean, temp_cold boolean, temp_chilly boolean, temp_warm boolean, temp_hot boolean
) 
stored as orc;

-- join the crimes and weather table
insert overwrite table haichenfu_crimes_and_weathers
select w.year as year, w.month as month, w.day as day, c.PrimaryType as primary_type,
w.mean_temperature as mean_temperature, w.mean_visibility as mean_visibility, w.mean_windspeed as mean_windspeed,
  w.fog as fog, w.rain as rain, w.snow as snow, w.hail as hail, w.thunder as thunder, w.tornado as tornado,
  w.visibility_clean as visibility_clean, w.visibility_moderate as visibility_moderate, w.visibility_low as visibility_low,
  w.visibility_poor as visibility_poor, w.temp_very_cold as temp_very_cold, w.temp_cold as temp_cold,
  w.temp_chilly as temp_chilly, w.temp_warm as temp_warm, w.temp_hot as temp_hot
from haichenfu_crimes c 
join haichenfu_weather w
on year(from_unixtime(unix_timestamp(c.crimedate, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS'))) = w.year
and month(from_unixtime(unix_timestamp(c.crimedate, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS'))) = w.month
  and day(from_unixtime(unix_timestamp(c.crimedate, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS'))) = w.day
and w.station = 725340;


select year, month, day, primary_type, mean_temperature 
from haichenfu_crimes_and_weathers limit 10;

SELECT COUNT(*) FROM haichenfu_crimes_and_weathers;