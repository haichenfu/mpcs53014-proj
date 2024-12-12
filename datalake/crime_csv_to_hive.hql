
-- map crime CSV data to hive table
create external table haichenfu_crimes_csv(
    Id string,
    CrimeDate string,
    PrimaryType string,
    LocationDescription string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/haichenfu/input'

TBLPROPERTIES ("skip.header.line.count" = "1");

-- testing command to check the haichenfu_crime_csv table
select id, crimedate from haichenfu_crimes_csv limit 5;

select id, casenumber, crimedate from haichenfu_crimes_csv limit 5;

select id from haichenfu_crimes_csv limit 10;

-- create an ORC table for crime data
create table haichenfu_crimes(
    Id int,
    CrimeDate string,
    PrimaryType string,
    LocationDescription string)
    stored as orc;

-- copy the csv table to the ORC table
insert overwrite table haichenfu_crimes select * from haichenfu_crimes_csv;
where crimedate is not null and PrimaryType is not null ;

-- testing command ot check the haichenfu_crime table
select id, crimedate from haichenfu_crimes limit 5;