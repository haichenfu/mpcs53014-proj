offset=0
limit=50000
total=8213533

while [ $offset -lt $total ]
do
    url="https://data.cityofchicago.org/resource/crimes.csv?\$select=id,date,primary_type,location_description&\$limit=$limit&\$offset=$offset"
    wget -O crimes_$offset.csv "$url"
    offset=$((offset+limit))
done