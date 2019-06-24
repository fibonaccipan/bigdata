// kafka多线程 多个消费者消费示例
package com.yowaqu.kafka

import java.time.Duration
import java.util.Properties
import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.mutable.ArrayBuffer

object MutiThreadConsumer {
  def main(args: Array[String]): Unit = {
    val consumerGroup = new ConsumerGroup(4,"MY_GROUP1","order-topic1",Seq("localhost:9092"))
    consumerGroup.execute()
  }
}
// 多线程消费类
class ConsumerGroup(val consumerNum:Int,val groupId:String,val topic:String,val brokerSeq:Seq[String]){
  private val consumers = new ArrayBuffer[ConsumerRunnable](consumerNum)
  for(i <- 0 until consumerNum)
    consumers += new ConsumerRunnable(brokerSeq,groupId,topic)

  def execute(): Unit ={
    consumers.foreach(new Thread(_).start())
  }
}

// 消费者处理 主类
class ConsumerRunnable(val brokerSeq:Seq[String],val groupId:String,val topic:String) extends Runnable{
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerSeq.mkString(","))
  props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
  val kafkaConsumer:KafkaConsumer[String,String] = new KafkaConsumer[String,String](this.props)
  this.kafkaConsumer.subscribe(util.Arrays.asList(topic))

  override def run(): Unit = {
    var cnt = 0
    while(true){
      val kafkaRecords = this.kafkaConsumer.poll(Duration.ofSeconds(1))
      val topicPartitions = kafkaRecords.partitions().iterator()
//      while(topicPartitions.hasNext){
//        val tp = topicPartitions.next()
//        val records = kafkaRecords.records(tp).iterator()
//        while(records.hasNext){
//          println(Thread.currentThread().getName," consumed partition:",
//            tp.toString,"the message with offset:",records.next().offset())
//        }
//      }
      val records2 = kafkaRecords.records(this.topic).iterator()
      while(records2.hasNext){
        val record2 = records2.next()
        println(Thread.currentThread().getName," consumed partition:",
          record2.partition()," the message with offset:",record2.offset())

      }
    }
  }
}