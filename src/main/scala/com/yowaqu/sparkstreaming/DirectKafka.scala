package com.yowaqu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
// 对于kafka 1.0 以后的版本 offset不保存在zk而是__consumer_offsets中的情况，使用
object DirectKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DirectKafka").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))
    val kafkaParam = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "MY_GROUP1",
      "auto.offset.reset" -> "latest", // use smallest or largest
      "enable.auto.commit" -> (true:java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,Subscribe[String,String](Array("order-topic1"),kafkaParam))
    stream.foreachRDD(_.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }
}
