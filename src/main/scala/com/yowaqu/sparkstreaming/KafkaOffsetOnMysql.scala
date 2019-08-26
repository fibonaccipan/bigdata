package com.yowaqu.sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *  KafkaOffsetOnMysql
  *
  * @Author fibonacci
  * @Description 使用Mysql 来管理groupid的offset 持久化 保证 exactly once 语义
  * @Date 2019/8/26
  * @Version 1.0
  */
object KafkaOffsetOnMysql {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("KafkaOffsetOnMysql")
          .setMaster("local[*]")
        val ssc = new StreamingContext(conf,Seconds(5))
        val kafkaParam = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "MysqlGroup1",
            "auto.offset.reset" -> "earliest", // use earliest or latest
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val topics=Array[String]("order-topic1")
        val offset = OffsetManager(topics.head,kafkaParam("group.id").toString)

        val Dstream =
            if(offset.nonEmpty) {
                KafkaUtils.createDirectStream[String,String](ssc,
                    LocationStrategies.PreferConsistent,
                    ConsumerStrategies.Subscribe[String,String](topics,kafkaParam,offset))
            } else {
                KafkaUtils.createDirectStream[String,String](ssc,
                    LocationStrategies.PreferConsistent,
                    ConsumerStrategies.Subscribe[String,String](topics,kafkaParam))
            }
        Dstream.foreachRDD(rdd => {
            val offsetRanges:Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // get offset
                offsetRanges.foreach(x => println(s"offset info: ${x}"))
            if(!rdd.isEmpty()){
                rdd.foreach(line => {
                    println(line.offset()+":"+line.value())
                })
                println("rdd has "+rdd.count()+" lines.")
            }
            OffsetManager.saveCurrentBatchOffset(kafkaParam("group.id").toString,offsetRanges)
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
