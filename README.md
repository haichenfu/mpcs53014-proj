# mpcs53014-proj
github repo: https://github.com/haichenfu/mpcs53014-proj
## Project description
This project build an application to display the relationship between crime and weather in Chicago. The datapoints covered in this dashboard are weather and crime data for Chicago from 2001 to 2024. User could query by year from the web app interface to check for averaged daily crime amount on the the specific weather condition for that year. 
![My Image](screenshots/web_app.png)
## Project implementation
### Part1 Data lake
#### 1.1 Crime data
https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data

The crime data is retrieved from the chicago data portal with API calls to save data as csv files into the cluster. The script is displayed as below and also can be found in `datalake/get_crime.sh`
```
offset=0
limit=50000
total=8213533

while [ $offset -lt $total ]
do
    url="https://data.cityofchicago.org/resource/crimes.csv?\$select=id,date,primary_type,location_description&\$limit=$limit&\$offset=$offset"
    wget -O crimes_$offset.csv "$url"
    offset=$((offset+limit))
done
```
Then the data is loaded from cluster to our hdfs file system with script `datalake/ingest_crime_to_hdfs.sh`
```
for name in *.csv
do
  echo "Uploading $name to HDFS..."
  hdfs dfs -put "$name" /haichenfu/input/
  echo "$name uploaded successfully."
done
```
Finally, the csv files in hdfs is stored into a hive table `haichenfu_crimes_csv` with script `datalake/crime_csv_to_hive.hql`
#### 1.2 Weather data
The weather data for flight and weather app is reused in the project. The weather data is in hive table `weathersummary`
### Part2 Batch Layer
#### 2.1 Weather by year
With weather data in hive table `weathersummary`, I further categorized the weather condition for temperature and visibility with script `batch_layer/refined_weather.hql` and save the refined weather result in hive table `haichenfu_weather`. I categorized the mean temperature into very_code, code, chilly, warm, and hot, and the visibility into clear, moderate, low, poor so that we would be able to  qualitatively reflect the relationship between crime and temperature, crime and visibility in our web app.  
```
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
```
With all weather conditions clearly categorized, it allows us to count the occurance of weather conditions in each year with script `batch_layer/weather_by_year.hql` and save the counted occurance of weather by year into hive table `haichenfu_weathers_by_year`. As we only cares about the weather in Chicago, I use the weather data collected from station 725340. 
```
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
```
#### 2.2 Crime by year
In order to know about the weather condition of each crime record, I joined the hive table `haichenfu_crime_csv` and `haichenfu_weather` on year, month, day (date in `haichenfu_crime_csv` table) with script `batch_layer/join_crime_to_weather.hql`. As mentioned before, the weather data collected by station 725340 is used for Chicago weather. 
```
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
```
With weather conditions for crime records processed, we are able to summarize the occurance of crime on each weather conditions by year with `batch_layer/crimes_by_year.hql` and save the result in hive table `haichenfu_crimes_by_year`. This table serves as the basis to be loaded into hbase for our serving layer. 
```
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
group by w.year, y.clear_count,y.fog_count, y.rain_count, y.snow_count, y.hail_count, y.thunder_count, y.tornado_count,y.visibility_clean_count, y.visibility_moderate_count, y.visibility_low_count, y.visibility_poor_count, y.temp_very_cold_count, y.temp_cold_count, y.temp_chilly_count, y.temp_warm_count, y.temp_hot_count;
```

### Part3 Serving Layer
### Part4 Web App
### Part5 Speed Layer
