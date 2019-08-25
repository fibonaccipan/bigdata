package com.yowaqu.sshbase

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}


/**
  * @ObjectName realTimeStatistics 
  * @Author fibonacci
  * @Description 统计实时流订单情况，汇总得到各级别区域内 实时销售情况 和 当日累计销售情况
  * @Date 19-7-30
  * @Version 1.0
  */
object realTimeStatistics {
    def main(args: Array[String]): Unit = {
        // init spark
        val appName = "realTimeStatistics"

        // init Kafka Param
        val configManager = new ConfigManager("conf")
        val kafkaParam = Map[String, Object](
            "bootstrap.servers" -> configManager.getProperty("bootstrap.servers"),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> configManager.getProperty("group.id.realtime"),
            "auto.offset.reset" -> configManager.getProperty("auto.offset.reset"), // use smallest or largest
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val interval = configManager.getProperty("ssh.interval").toInt
        val topic:String = configManager.getProperty("result.topics")
        val path:String = configManager.getProperty("path.checkpoint.realtime")

        val ssc = StreamingContext.getOrCreate(path,() => createContextAndCompute(configManager,kafkaParam,topic,appName,interval,path))

        ssc.start()
        ssc.awaitTermination()
    }

    def createContextAndCompute(cfgMngr:ConfigManager,kfkPrm:Map[String,Object],tpc:String,appNm:String,interval:Int,path:String):StreamingContext={
        // init Spark Streaming Context
        println("this is first time to create Spark Streaming Context")
        val conf = new SparkConf().setAppName(appNm)
          .setMaster("local[*]")
        // http://spark.apache.org/docs/2.3.0/streaming-kafka-0-10-integration.html 中提到当spark 的 batch duration 超过 kafka 默认的心跳超时时间（30s）
        // 需要配置kafka的 heartbeat.interval.ms 和 session.timeout.ms 如果 batch duration 超过 5分钟 需要修改 broker上 group.max.session.timeout.ms
        val ssc = new StreamingContext(conf,Minutes(interval))
//        val ssc = new StreamingContext(conf,Seconds(10))
        // 指定　checkpoint
        ssc.checkpoint(path)
        //从kafka　获得数据
        val Dstream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,Subscribe[String,String](Array(tpc),kfkPrm))
        // 对每一条记录　变为　map(时间戳,order)
        val ObjDStream = Dstream.map(line =>{
            val msgHandler = new MsgHandler()
            if(msgHandler.handlerMsg(line.value(),cfgMngr)){
                val order = msgHandler.getOrderBean(line.value())
                if(order != null)
                    (new SimpleDateFormat("yyyyMMddHH").format(order.getTime.toLong),order)
                else
                    null
            }else{
                null
            }
        }).filter(_ != null)
            .window(Minutes(cfgMngr.getProperty("ssh.window.interval").toInt))

        val currOrdDStm = ObjDStream
            .filter(_._1 == new SimpleDateFormat("yyyyMMddHH").format(new Date())) // 去除掉非　本小时段的订单
//            .map(_._2)
        currOrdDStm.persist(StorageLevel.MEMORY_ONLY)

//        currOrdDStm.map(line => (line._1 + "-" + line._2.getCity_code,line._2.getPay_amount.toFloat))
//            .foreachRDD(_.foreach(x=>println(x._1,x._2)))
//
//        currOrdDStm.map(line => (line._1 + "-" + line._2.getCity_code,line._2.getPay_amount.toDouble))
//            .foreachRDD(_.foreach(println))


        currOrdDStm.map(line => (line._1 + "-" + line._2.getCity_code,line._2.getPay_amount.toDouble))
          .reduceByKey(_+_)
          .foreachRDD(_.foreach(line => {
              val hbaseUtils = new HbaseUtils(cfgMngr.getProperty("zk.quorum"), cfgMngr.getProperty("zk.port"))
              hbaseUtils.insertOneRow("sale_statistic",line._1,"colFmly",Map("sumAmount"->line._2.toString))
          }))



        Dstream.foreachRDD(rdd =>{
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            Dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        })
        ssc
    }
}
