# put files to hdfs

for name in *.csv
do
  echo "Uploading $name to HDFS..."
  hdfs dfs -put "$name" /haichenfu/input/
  echo "$name uploaded successfully."
done