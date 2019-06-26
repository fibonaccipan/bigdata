//多线程worker方式 使用一个Consumer接收消息放入队列由多个work处理。
//相对比较好，将业务处理从Consumer中剥离出来了。有效避免了 rebalance超时间、coordinate重新选举，心跳无法维持的问题
package com.yowaqu.kafka

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.{ArrayBlockingQueue, ExecutorService, ThreadPoolExecutor, TimeUnit}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}

object MutiWorkerConsumer {
  def main(args: Array[String]): Unit = {
    val consumers = new ConsumerHandler(Seq("localhost:9092"),"MY_GROUP1","order-topic1")
    consumers.execute(5)
    try{
      Thread.sleep(1000000)
    } catch {
      case e:InterruptedException => println(e)
    }
    consumers.shutdown()
  }
}

class Worker(val record:ConsumerRecord[String,String]) extends Runnable{
  override def run(): Unit = {
    println(Thread.currentThread().getName," consumed ", record.partition(),"the message with offset:",record.offset())
  }
}

class ConsumerHandler(brokerSeq:Seq[String],groupId:String,val topic:String){
  val props = new Properties()
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerSeq.mkString(","))
  props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer")
  val kafkaConsumer:KafkaConsumer[String,String] = new KafkaConsumer[String,String](this.props)
  this.kafkaConsumer.subscribe(util.Arrays.asList(this.topic))
  var executors:ExecutorService = _

  def execute(workerNum:Int): Unit ={
    this.executors = new ThreadPoolExecutor(workerNum,
                    workerNum,
      0L,TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue[Runnable](1000),
                    new ThreadPoolExecutor.CallerRunsPolicy)
    while(true){
      val kafkaRecords = kafkaConsumer.poll(Duration.ofMillis(100))
      val records = kafkaRecords.records(this.topic).iterator()
      while(records.hasNext){
        this.executors.submit(new Worker(records.next()))
      }
    }
  }

  def shutdown():Unit={
    if(this.kafkaConsumer != null)
      kafkaConsumer.close()
    if(this.executors != null)
      executors.shutdown()
    try{
      if(executors.awaitTermination(10,TimeUnit.SECONDS)){
        println("Timeout  ...  Ignore for this case")
      }
    }catch{
      case ignored:InterruptedException =>
        println("Other thread interrupted this shutdown ,ignore for this case.", ignored)
        Thread.currentThread().interrupt()
    }
  }
}
