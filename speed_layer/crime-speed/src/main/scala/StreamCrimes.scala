import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamCrimes {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val crimeByWeather = hbaseConnection.getTable(TableName.valueOf("haichenfu_crimes_by_years_hbase"))
  val latestWeather = hbaseConnection.getTable(TableName.valueOf("haichenfu_latest_weather"))
  
  def getLatestWeather() = {
      val result = latestWeather.get(new Get(Bytes.toBytes("725340")))
      if(result.isEmpty())
        None
      else
        Some(WeatherReport(
              Bytes.toDouble(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("temperature"))),
              Bytes.toDouble(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("visibility"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("fog"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("rain"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("snow"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("hail"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("thunder"))),
              Bytes.toBoolean(result.getValue(Bytes.toBytes("weather"), Bytes.toBytes("tornado")))))
  }
  
  def incrementCrimeByWeather(kfr : KafkaCrimeRecord) : String = {
    val maybeLatestWeather = getLatestWeather()
    if(maybeLatestWeather.isEmpty) {
      System.out.println("No latest weather available")
      return "No latest weather available"
    }
    val latestWeather = maybeLatestWeather.get
    val year: String = kfr.date.split("-")(0)
    val inc = new Increment(Bytes.toBytes(year))
    if(latestWeather.clear) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("clear_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("clear_count"), 1L)
    }
    if(latestWeather.fog) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("fog_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("fog_count"), 1L)
    }
    if(latestWeather.rain) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("rain_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("rain_count"), 1L)
    }
    if(latestWeather.snow) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("snow_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("snow_count"), 1L)
    }
    if(latestWeather.hail) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("hail_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("hail_count"), 1L)
    }
    if(latestWeather.thunder) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("thunder_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("thunder_count"), 1L)
    }
    if(latestWeather.tornado) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("tornado_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("tornado_count"), 1L)
    }
    if(latestWeather.visibility_clear) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("visibility_clear_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("visibility_clear_count"), 1L)
    }
    if (latestWeather.visibility_moderate) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("visibility_moderate_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("visibility_moderate_count"), 1L)
    }
    if (latestWeather.visibility_low) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("visibility_low_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("visibility_low_count"), 1L)
    }
    if (latestWeather.visibility_poor) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("visibility_poor_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("visibility_poor_count"), 1L)
    }
    if(latestWeather.temp_very_cold) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_very_cold_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_very_cold_count"), 1L)
    }
    if (latestWeather.temp_cold) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_cold_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_cold_count"), 1L)
    }
    if (latestWeather.temp_chilly) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_chilly_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_chilly_count"), 1L)
    }
    if (latestWeather.temp_warm) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_warm_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_warm_count"), 1L)
    }
    if (latestWeather.temp_hot) {
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_hot_crime"), 1L)
      inc.addColumn(Bytes.toBytes("crime"), Bytes.toBytes("temp_hot_count"), 1L)
    }
    crimeByWeather.increment(inc)
    return "Updated speed layer for crime"
  }
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamCrimes <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }
    
    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamCrimes")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("haichenfu-crime-report")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "crime_speed",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaCrimeRecord]))

    // Update speed table    
    val processedCrimes = kfrs.map(incrementCrimeByWeather)
    processedCrimes.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
