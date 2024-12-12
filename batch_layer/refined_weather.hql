create table haichenfu_weather (
    year smallint, month tinyint, day tinyint, station string,
    mean_temperature double, mean_visibility double, mean_windspeed double,
  fog boolean, rain boolean, snow boolean, hail boolean, thunder boolean, tornado boolean,
  visibility_clean boolean, visibility_moderate boolean, visibility_low boolean, visibility_poor boolean,
  temp_very_cold boolean, temp_cold boolean, temp_chilly boolean, temp_warm boolean, temp_hot boolean
) 
stored as orc;

insert overwrite table haichenfu_weather 
select w.year as year, w.month as month, w.day as day, w.station as station,
w.meantemperature as mean_temperature, w.meanvisibility as mean_visibility, w.meanwindspeed as mean_windspeed,
  w.fog as fog, w.rain as rain, w.snow as snow, w.hail as hail, w.thunder as thunder, w.tornado as tornado,
  if (w.meanvisibility >= 30, true, false) as visibility_clean,
  if (w.meanvisibility < 30 and w.meanvisibility >= 10, true, false) as visibility_moderate,
  if (w.meanvisibility < 10 and w.meanvisibility >= 2, true, false) as visibility_low,
  if (w.meanvisibility < 2, true, false) as visibility_poor,
  if (w.meantemperature < 32, true, false) as temp_very_cold,
  if (w.meantemperature >= 32 and w.meantemperature < 50, true, false) as temp_cold,
  if (w.meantemperature >= 50 and w.meantemperature < 65, true, false) as temp_chilly,
  if (w.meantemperature >= 65 and w.meantemperature < 80, true, false) as temp_warm,
  if (w.meantemperature >= 80, true, false) as temp_hot 
from weathersummary w;
