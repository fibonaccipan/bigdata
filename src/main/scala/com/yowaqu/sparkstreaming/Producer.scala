package com.yowaqu.sparkstreaming

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord,ProducerConfig}

import scala.io.Source
import org.json.JSONObject

object Producer {
  def main(args: Array[String]): Unit = {
    // 设置kafka 参数
    val props = new Properties()
    // 设置 消息对partition的leader和replica 投递可靠性。
    // -1：所有replica都返回确认消息 ， 1：仅leader确认，replica异步拉取消息 ， 0：不等broker确认信息
    props.put(ProducerConfig.ACKS_CONFIG,"-1")
    // 设置kafka集群地址
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
    //失败重试次数
    props.put(ProducerConfig.RETRIES_CONFIG,"0")
    // 每个分区未发生消息总字节大小，超过设置值就提交
    props.put(ProducerConfig.BATCH_SIZE_CONFIG,"10")
    // 消息在缓冲区 保留时间，超过阀值就发送
    props.put(ProducerConfig.LINGER_MS_CONFIG,"10000")
    // 整个producer 内存大小，如果满了也会提交，注意要大于 所有分区的缓冲区总和
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"10240")
    // 设置 序列化器
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    // 初始化 生产者
    val producer = new KafkaProducer[String,String](props)
    // 设置 文件读取参数
    val file = Source.fromFile("/home/fibonacci/data/order.txt")
    var lines = file.getLines().toArray
    file.close()
    implicit val jsonHeader = new JSONHeader(lines.head) //设置json头为隐式值
    lines = lines.drop(1)
    val ods = new Orders(lines)
//    ods.jsonHead.foreach(println)

    var i = 0
    while(true){
      //  json对象转为String 并使用send发送出去
      val tmpjson = ods.randomJson
      producer.send(new ProducerRecord("order-topic1",tmpjson.toString))
      Thread.sleep(13000)
      i+=1
      println(i)
    }
    producer.close()
  }
}

class Orders(){
  var jsonHead:Array[String] = _
  var jsonOrders:Array[String] = _
  private[this] def randomOrder= {
    if(jsonOrders.length > 0)
      jsonOrders((new util.Random).nextInt(jsonOrders.length))
    else
      new String("")
  }

  def this(order:Array[String])(implicit header:JSONHeader){
    this()
    jsonOrders = order
    jsonHead = header.value
  }

  def randomJson={
    val tmpJson = new JSONObject()
    val jsonOrder = randomOrder
    for(i <- jsonHead.indices){
      tmpJson.put(jsonHead(i),jsonOrder.split("\t")(i))
    }
    tmpJson
  }
}

class JSONHeader(line:String){
  val value:Array[String] = line.split("\t")
}