package com.yowaqu.sshbase

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderHandler {
  def main(args: Array[String]): Unit = {
    val configManager = new ConfigManager("conf")
    val conf = new SparkConf().setAppName(configManager.getProperty("streaming.appName"))
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(configManager.getProperty("streaming.interval").toInt))
    val kafkaParam = Map[String,Object](
      "bootstrap.servers" -> configManager.getProperty("bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> configManager.getProperty("group.id"),
      "auto.offset.reset" -> configManager.getProperty("auto.offset.reset"), // use smallest or largest
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val props = new Properties()
    props.setProperty("metadata.broker.list",configManager.getProperty("metadata.broker.list"))
//    implicit val formats = DefaultFormats
    val Dstream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array("order-topic1"), kafkaParam))
    Dstream.foreachRDD(
      rdd =>{
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        if(!rdd.isEmpty()){
            val msgHandler = new MsgHandler()
            val cleanStreamRdd:RDD[(String,Order)] = rdd.mapPartitions(iter => {
                iter.map(line =>{
                    if(msgHandler.handlerMsg(line.value(),configManager)){
                        val order = msgHandler.getOrderBean(line.value())
                        if(order != null)
                            (order.getId,order)
                        else
                            null
                    }else{
                        null
                    }
                })
            }).filter(_ != null)
            cleanStreamRdd.foreachPartition(_.foreach(x => println(x._2.getPay_amount)))
        }
        Dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
