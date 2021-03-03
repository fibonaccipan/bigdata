package com.yowaqu.kafka

// 参考 https://blog.csdn.net/wangzhanzheng/article/details/80801059
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json.JSONObject

import scala.io.Source

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
    implicit val jsonHeader:JSONHeader = new JSONHeader(lines.head) //设置json头为隐式值
    lines = lines.drop(1)
    val ods = new Orders(lines)
//    ods.jsonHead.foreach(println)
// init mysql param
    val mysqlConf=Map[String,String](
     "driver"->"com.mysql.jdbc.Driver",
     "url"->"jdbc:mysql://127.0.0.1:3306/bigdata?autoReconnect=true",
     "username"->"bigdata",
     "password"->"bigdata"
    )
    Class.forName(mysqlConf("driver"))
    val conn = DriverManager.getConnection(mysqlConf("url"),mysqlConf("username"),mysqlConf("password"))
    val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_UPDATABLE)
    var i = 0
    while(true){
        Thread.sleep(1200)
        //  json对象转为String 并使用send发送出去
      val tmpjson = ods.randomJson
//      producer.send(new ProducerRecord[String,String]("order-topic",0,null,tmpjson.toString)) //直接指定分区
      producer.send(new ProducerRecord("order-topic1",tmpjson.get("area_code").toString,tmpjson.toString)) //使用area_code 作为key保证分区内数据有序性
//      更多的 分区指定的问题可以通过 实现Partitioner接口 重写一个类，使用props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"xxx")
//      producer.send(new ProducerRecord("order-topic1",tmpjson.toString)) //随机分配卡夫卡分区
      insert(conn,tmpjson.get("id").toString,
          tmpjson.get("order_itemid").toString,
          tmpjson.get("city_code").toString,
          tmpjson.get("general_gds_code").toString,
          tmpjson.get("pay_amount").toString,
          tmpjson.get("time").toString)
      i+=1
      if(i<10)
        println(tmpjson)
      else
        println(i)
    }
    producer.close()
  }
  def insert(conn: Connection,id:String,orderItemId:String,cityCode:String,gdsCode:String,payAmount:String,time:String): Unit ={
    try{
      val prep = conn.prepareStatement("insert into order_info (id,order_itemid,city_code,gds_code,pay_amount,time) values (?,?,?,?,?,?)")
      prep.setString(1,id)
      prep.setString(2,orderItemId)
      prep.setString(3,cityCode)
      prep.setString(4,gdsCode)
      prep.setString(5,payAmount)
      prep.setString(6,new SimpleDateFormat("yyyyMMddHHmm").format(time.toLong))
      prep.executeUpdate()
    } catch {
        case e:Exception => e.printStackTrace()
    }
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

  def randomJson:JSONObject = {
    val tmpJson = new JSONObject()
    val jsonOrder = randomOrder
    for(i <- jsonHead.indices if Array(0,1,2,4,6,7,8,9,10,13).contains(i)){
      tmpJson.put(jsonHead(i),jsonOrder.split("\t")(i))
    }
    tmpJson.put("time",new Date().getTime.toString)
//    tmpJson.put("city_code","025")
    tmpJson
  }
}

class JSONHeader(line:String){
  val value:Array[String] = line.split("\t")
}

