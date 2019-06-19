package com.yowaqu.kafka

import java.time.Duration
import java.util
import java.util.{Arrays, HashMap, Properties,Map}

//import kafka.common.OffsetAndMetadata
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer,OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable.ArrayBuffer
//https://blog.csdn.net/wangzhanzheng/article/details/80801059

object Consumer {
  def main(args: Array[String]): Unit = {
    val consumer = new Consumer(Seq("localhost:9092"),"order-topic1","MY_GROUP1")
//    consumer.autoCommit
//    consumer.manualCommitCnsmr
//    consumer.manualCommitPartit
    consumer.manualCommitPartitAll
  }
}

class Consumer(brokerSeq:Seq[String],topic:String,groupId:String){
  val props = new Properties()
  var kafkaConsumer:KafkaConsumer[String,String] = _
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerSeq.mkString(","))
  props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
  //props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY,"range") // 分配策略 待详细了解
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
//  props.setProperty("bootstrap.servers","localhost:9092")
//  props.setProperty("group.id",groupId)
//  props.setProperty("enable.auto.commit","true")
//  props.setProperty("auto.commit.interval.ms","1000")
//  props.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//  props.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")


//  offset 按分区消费提交
  def manualCommitPartitAll:Unit={
    //关闭 offset 自动提交
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
    //实例化kfk消费者
    kafkaConsumer = new KafkaConsumer[String,String](props)
    //消费者订阅主题
    kafkaConsumer.subscribe(Arrays.asList(this.topic))
    val batchSize:Int = 2 //设置缓存区大小
    val buffer = ArrayBuffer[ConsumerRecord[String,String]]()
    try{
      while(true){
        // 指定拉取间隔
        val kafkaRecodes = kafkaConsumer.poll(Duration.ofSeconds(3))
        // 得到 topic的 TopicPartition 对象集合
        val topicPartition =kafkaRecodes.partitions().iterator()
        //循环 topicPartition对象 即 每个  主题的 每个分区
        while(topicPartition.hasNext){
          // 获取到 分区 对象
          val tp = topicPartition.next()
          // 获取到这个分区的 数据 并集合化
          val recodes = kafkaRecodes.records(tp).iterator()
          // 将集合内容转到数组内
          while(recodes.hasNext)
            buffer += recodes.next()
          //消费超出 缓冲区 则 执行提交 offset 并操作
          if(buffer.length >= batchSize){
            buffer.foreach(r => println(s"partition= ${r.partition()},offset= ${r.offset},key= ${r.key},value=${r.value}"))
            val offset:Long = buffer.last.offset()+1
            val map:Map[TopicPartition,OffsetAndMetadata] = new HashMap()
            map.put(tp,new OffsetAndMetadata(offset))
            kafkaConsumer.commitSync(map)
            buffer.clear()
          }
        }
      }
    }finally {
      kafkaConsumer.close()
    }
  }

//  offset 手动按分区提交
  def manualCommitPartit={
    //关闭 offset 自动提交
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
    //实例化kfk消费者
    kafkaConsumer = new KafkaConsumer[String,String](props)
    //消费者订阅主题
    kafkaConsumer.subscribe(Arrays.asList(this.topic))
    val batchSize:Int = 10 //设置缓存区大小
    val buffer = ArrayBuffer[ConsumerRecord[String,String]]()
    try{
      while(true){
        //设置消费者 拉取数据频率
        val kafkaRecodes = kafkaConsumer.poll(Duration.ofSeconds(3))
//        println(kafkaRecodes.partitions())
        // 使用TopicPartition()对象指定获取的topic和partition
        val records = kafkaRecodes.records(new TopicPartition(topic,0)).iterator()
        while(records.hasNext)
          buffer += records.next()
        if(buffer.length >= batchSize){
          val offset:Long = buffer.last.offset() // offset 需要加一，
          val tp = new TopicPartition(topic,0)
          val ofst = new OffsetAndMetadata(offset)
          val map:Map[TopicPartition,OffsetAndMetadata] = new HashMap()
          map.put(tp,ofst)
          buffer.foreach(r => println(s"partition= ${r.partition()},offset= ${r.offset},key= ${r.key},value=${r.value}"))
          //使用TopicPartition 和 offsetAndMetaData对象 构建map 提交offset
          kafkaConsumer.commitSync(map)
          buffer.clear()
        }
      }
    } finally {
      kafkaConsumer.close()
    }
  }
//  offset 手动按消费者提交
  def manualCommitCnsmr={
    //设置关闭自动提交
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false")
    //实例化kfk消费者
    kafkaConsumer = new KafkaConsumer[String,String](props)
    //消费者订阅主题
    kafkaConsumer.subscribe(Arrays.asList(this.topic))
    val batchSize:Int = 33 //设置缓存区大小
    val buffer = ArrayBuffer[ConsumerRecord[String,String]]()
    while(true){
      val kafkaRecodes = kafkaConsumer.poll(Duration.ofSeconds(10))
      val recodes = kafkaRecodes.records(this.topic).iterator
      while(recodes.hasNext){
        buffer += recodes.next()
      }
      if(buffer.length >= batchSize){
        buffer.foreach(r => println(s"partition= ${r.partition()},offset= ${r.offset},key= ${r.key},value=${r.value}"))
        kafkaConsumer.commitAsync()
        buffer.clear
      }
    }
  }
//  offset 自动提
  def autoCommit ={
    //设置自动提交参数
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000")
    //实例化kafka消费者
    kafkaConsumer = new KafkaConsumer[String,String](props)
    //消费者订阅主题
    kafkaConsumer.subscribe(Arrays.asList(this.topic))
    while(true){
      val kafkaRecodes = kafkaConsumer.poll(Duration.ofMillis(10000))
      val recodes = kafkaRecodes.records(topic).iterator
      while(recodes.hasNext){
        val recode = recodes.next()
        println(s"offset= ${recode.offset},key= ${recode.key},value= ${recode.value}")
      }
    }
  }
}


