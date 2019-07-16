package com.yowaqu.sshbase

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderHandler {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("orderHandler")
      .setMaster("local[3]")
    val ssc = new StreamingContext(conf,Seconds(10))
    val kafkaParam = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "MY_GROUP1",
      "auto.offset.reset" -> "earliest", // use smallest or largest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
//    implicit val formats = DefaultFormats
    val Dstream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array("order-topic1"), kafkaParam))
    Dstream.foreachRDD(
      rdd =>{
        val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        if(!rdd.isEmpty()){
          val thousandRdd = //:RDD[(String,JSONObject)] =
            rdd.mapPartitions({
            iter => {
              iter.map(x => (s"${x.partition()}-${x.offset()}",JSONObject)
            }
          })
          thousandRdd.foreach(x => println(x._1,x._2))
        }
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
