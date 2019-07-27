package com.yowaqu.sshbase

import java.util.Properties

import com.alibaba.fastjson.{JSON}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderHandler {
    def main(args: Array[String]): Unit = {
        //      init spark
        val configManager = new ConfigManager("conf")
        val conf = new SparkConf().setAppName(configManager.getProperty("streaming.appName"))
          .setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(configManager.getProperty("streaming.interval").toInt))

        //      init kafkaConsumerParam
        val kafkaParam = Map[String, Object](
            "bootstrap.servers" -> configManager.getProperty("bootstrap.servers"),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> configManager.getProperty("group.id"),
            "auto.offset.reset" -> configManager.getProperty("auto.offset.reset"), // use smallest or largest
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        //      init kafkaProducerParam
        val props = new Properties()
        props.put(ProducerConfig.ACKS_CONFIG, "-1")
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configManager.getProperty("bootstrap.servers"))
        props.put(ProducerConfig.RETRIES_CONFIG, "0")
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "10")
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10000")
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "10240")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

        // HbaseConfiguration is a connection pool, can't Serializable. should init at every worker
        //val hbaseUtils = new HbaseUtils(configManager.getProperty("zk.quorum"),configManager.getProperty("zk.port"))
        //分布式Hbase 无法序列化 参考https://stackoverflow.com/questions/25250774/writing-to-hbase-via-spark-task-not-serializable

        // init producer
        //val producer = new KafkaProducer[String, String](props) with Serializable

        val Dstream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array(configManager.getProperty("input.topics")), kafkaParam))
        Dstream.foreachRDD(
            rdd => {
                val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                if (!rdd.isEmpty()) {
                    val msgHandler = new MsgHandler()
                    val cleanStreamRdd: RDD[(String, Order)] = rdd.mapPartitions(iter => {
                        val hbaseUtils = new HbaseUtils(configManager.getProperty("zk.quorum"), configManager.getProperty("zk.port"))
                        iter.map(line => {
                            if (msgHandler.handlerMsg(line.value(), configManager)) {
                                val order = msgHandler.getOrderBean(line.value())
                                if (order != null) {
                                    val city_name = hbaseUtils.getRowMap("city", order.getCity_code, "colFmly")("city_nm")
                                    order.setCity_name(city_name)
                                    (order.getId, order)
                                }
                                else
                                    null
                            } else {
                                null
                            }
                        })
                    }).filter(_ != null)
                    // output new order info into another kafka topic
                    //                    cleanStreamRdd.foreachPartition(_.foreach(x => println(x._2.getCity_code, x._2.getCity_name)))
                    cleanStreamRdd.foreachPartition(
                        _.foreach(x => { // x is map[String,Order](order.getId,order)
                            val producer = new KafkaProducer[String, String](props)
                            producer.send(new ProducerRecord(configManager.getProperty("output.topics"),x._1,JSON.toJSONString(x._2,false)))
                            producer.close()
//                            println(x._1 , JSON.toJSONString(x._2,false))
                        })
                    )
                }
//                Dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            }
        )
        // close producer
//        producer.close()

        // start Spark Streaming
        ssc.start()
        ssc.awaitTermination()
    }

}
