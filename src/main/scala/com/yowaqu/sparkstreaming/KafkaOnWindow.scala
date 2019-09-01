package com.yowaqu.sparkstreaming

import com.yowaqu.sshbase.ConfigManager
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * @ObjectName KafkaOnWindow 
  * @Author fibonacci
  * @Description 使用 spark streaming 的窗口 去统计kafka数据，并且使用mysql手动管理offset
  * @Date 2019/8/26
  * @Version 1.0
  */
object KafkaOnWindow {
    def main(args: Array[String]): Unit = {
        //init spark info
        val appName = "computeByWindow"

        //init kafka param
        val kafkaParam = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "WindowGroup",
            "auto.offset.reset" -> "latest", // use earliest or latest
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val topics=Array[String]("order-topic1")
        // init mysql param
//        val offset = OffsetManager(topics.head,kafkaParam("group.id").toString)
        val checkPointPath="hdfs://localhost:9000/spark/streaming/computeByWindow"
        val ssc = StreamingContext.getOrCreate(checkPointPath,() =>createContextAndCompute(appName,checkPointPath,kafkaParam,topics))
        ssc.start()
        ssc.awaitTermination()
    }

    def createContextAndCompute(appName:String,ckptPath:String,kfkPm:Map[String,Object],tpc:Array[String]):StreamingContext={
        // init Spark Streaming Context
        println("this is first time to create Spark Streaming Context !")
        val conf = new SparkConf().setAppName(appName).setMaster("local[*]")
        val ssc = new StreamingContext(conf,Minutes(1))
        // set checkPoint
        ssc.checkpoint(ckptPath)
        // create Dstream from Kafka use Direct method
        val offsetRanges = OffsetManager(tpc.head,kfkPm("group.id").toString)
        val DStream =
            if(offsetRanges.nonEmpty){
                KafkaUtils.createDirectStream[String,String](ssc,
                    LocationStrategies.PreferConsistent,
                    ConsumerStrategies.Subscribe[String,String](tpc,kfkPm,offsetRanges)
                )
            }else{
                KafkaUtils.createDirectStream[String,String](ssc,
                    LocationStrategies.PreferConsistent,
                    ConsumerStrategies.Subscribe[String,String](tpc,kfkPm))
            }
        import scala.collection.mutable
        val offset:mutable.HashMap[String,Array[OffsetRange]] = new mutable.HashMap()
        // use transform get every batch's offsetRanges
        val WStream = DStream
          .transform(rdd => {
            val rddOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            offset += "rdd" -> rddOffsetRanges
            rdd.mapPartitions(iter =>{
                iter.map(line =>{
                    line.value
                })
            })
        })
        // use window make it be WindowStream
          .window(Minutes(3))

        WStream.foreachRDD(rdd =>{
            if(!rdd.isEmpty()){
                val rddOffsetRanges = offset("rdd")
                rdd.foreachPartition(iter =>{
                    iter.foreach(line =>{
                        println(line)
                    })
                })
                println("rdd count is :"+rdd.count())
                offsetRanges.foreach(println)
                OffsetManager.saveCurrentBatchOffset(kfkPm("group.id").toString,rddOffsetRanges)
            }
        })
        ssc
    }
}
