package com.yowaqu.sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


//使用spark streaming 对kafka 按分区消费 手动管理offsets
object DirectKafkaManualOffset {
  val LOG = LoggerFactory.getLogger(DirectKafkaManualOffset.getClass.getName)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DirectKafkaManualOffset")
      .setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val kafkaParam = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "MY_GROUP1",
      "auto.offset.reset" -> "earliest", // use smallest or largest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    LOG.info("######Streaming start###########")
    val Dstream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array("order-topic1"), kafkaParam))
    Dstream.foreachRDD(rdd =>{
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(iter =>{
        iter.foreach(line =>println(line.value()))
      })
      Dstream.asInstanceOf[CanCommitOffsets].commitAsync((offsetRanges))
      println(Thread.currentThread().getName)
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
