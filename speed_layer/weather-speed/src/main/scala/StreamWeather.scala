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
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object StreamWeather {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()


  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("haichenfu_latest_weather"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamWeather <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamWeather")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("haichenfu-weather-report")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "weather_speed",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[WeatherReport]))

    // How to write to an HBase table
    val batchStats = reports.map(wr => {
      val put = new Put(Bytes.toBytes("725340"))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("clear"), Bytes.toBytes(wr.clear))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("fog"), Bytes.toBytes(wr.fog))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("rain"), Bytes.toBytes(wr.rain))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("snow"), Bytes.toBytes(wr.snow))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("hail"), Bytes.toBytes(wr.hail))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("thunder"), Bytes.toBytes(wr.thunder))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("tornado"), Bytes.toBytes(wr.tornado))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("temperature"), Bytes.toBytes(wr.temperature))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("visibility"), Bytes.toBytes(wr.visibility))
      table.put(put)
    })
    batchStats.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
