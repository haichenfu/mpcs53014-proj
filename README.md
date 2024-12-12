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
Finally, the csv files in hdfs is stored into a hive table with script `datalake/crime_csv_to_hive.hql`
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
With all weather conditions clearly categorized, it allows us to count the occurance of weather conditions in each year with script `batch_layer/weather_by_year.hql` and save the counted occurance of weather by year into hive table `haichenfu_weathers_by_year`
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

### Part3 Serving Layer
### Part4 Web App
### Part5 Speed Layer
