package com.yowaqu.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**基于Receiver的方式 消费kafka，receiver 是spark streaming 的接收器
  * Receiver 和 Direct 方式的区别
  * 1.Receiver通过ZK接入kafka,Direct 接broker上的partition
  * 2.Receiver通过kafka的高级API来实现获取数据，并存入spark executor的内存中然后由spark streaming去启动job处理。
  *     所以可能会因为底层失败而导致丢失数据，因此需要开启WAL(Write Ahead Log)预写日志机制，以便失败恢复
  * 3.
 */
/** 解决的问题 1.系统报错找不到Logging类，解决方法，下载对应的包 导入到项目中，可能会在后期遇见maven依赖非本地repository包，编译报错找不到符号。
  * 未解决问题 2.Receiver 方式,对 kafka 消费不基于offset 数据丢失 无法重新消费，无法对历史消费。
  * */
object ReceiverKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReceiverKafka").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(2))
//    val record = KafkaUtils.create // 由于kafka 0.10 以后放弃了在zookeeper保存offset，所以spark streaming 的高级api 被废弃了
//    record.map(_._2).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
