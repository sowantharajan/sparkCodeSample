import scala.collection.mutable

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

case class wOffsetSchema(offset_ :String, topic_ : String, partition_ : String)

object SparkStreamKafka {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("sparkApp")
    .getOrCreate()

  import spark.sql
  import spark.implicits._

  var fromOffsets = mutable.Map.empty[TopicPartition, Long]
  val results = spark.sparkContext.broadcast(fromOffsets)
  val sc = spark.sparkContext
  val data = spark.sparkContext.parallelize(Seq(wOffsetSchema("1001", "topic1", "0"), wOffsetSchema("1001", "topic1", "1"))).toDS()

  data.foreachPartition {
    iter =>
      iter.foreach {
        s => results.value += (new TopicPartition(s.topic_, s.partition_.toInt) -> s.offset_.toLong)
      }

      fromOffsets = results.value
      if (fromOffsets.isEmpty) {
        fromOffsets += (new TopicPartition("demo", 0) -> 1234l)
      }

      val kafkaParams = Map(
        "group.id" -> "test",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "metadata.broker.list" -> "hklvadcnc03.hk.standardchartered.com:6667,hklvadcnc06.hk.standardchartered.com:6667",
        "zookeeper.connect" -> "10.20.174.132")

      val sc = new StreamingContext(spark.sparkContext, Seconds(5))
      val line = KafkaUtils.createDirectStream[String, String](
        sc,
        PreferConsistent,
        ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))

      line.foreachRDD {
        rdd =>
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          offsetRanges.foreach {
            o => println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          }
      }

      sc.start()
      sc.awaitTermination()
      sc.stop()
  }

}


<dependencies>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-core_2.11</artifactId>
			<version>2.2.0.2.6.4.68-1</version>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-sql_2.11</artifactId>
			<version>2.2.0.2.6.4.68-1</version>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-hive_2.11</artifactId>
			<version>2.2.0.2.6.4.68-1</version>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-streaming_2.11</artifactId>
			<version>2.2.0.2.6.4.68-1</version>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-streaming-kafka-0-10_2.11</artifactId>
			<version>2.2.0.2.6.4.68-1</version>
		</dependency>
		<dependency>
			<groupId>mysql</groupId>
			<artifactId>mysql-connector-java</artifactId>
			<version>5.1.35</version>
		</dependency>
	</dependencies>
