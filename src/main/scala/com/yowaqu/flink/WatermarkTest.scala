package com.yowaqu.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ ObjectName WatermarkTest
  * @ Author fibonacci
  * @ Description TODO
  * @ Date 19-8-17
  * @ Version 1.0
  *  https://www.cnblogs.com/rossiXYZ/p/12286407.html
  *
  *  data: hello,1553503185000,1    hello,1553503186000,2
  */
object WatermarkTest {
    def main(args: Array[String]): Unit = {
        val hostName = "localhost"
        val port:Int = 7778
        val env = StreamExecutionEnvironment.getExecutionEnvironment
//        设置并行度为 1
        env.setParallelism(1)
//        设置为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//        接受套接字内容
        val inputStrm = env.socketTextStream(hostName,port).filter(_.nonEmpty)
        val eventStrm = inputStrm.flatMap(_.split("\\s|\\r"))
          .map(x => {val arr = x.split(",");MyEvent(arr(0),arr(1).toLong,arr(2).toInt)})
          .assignTimestampsAndWatermarks(new MyPunctuatedWaterMark())

        eventStrm.keyBy(_.name)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
          .reduce((x,y)=> MyEvent(x.name,Math.max(x.timeStamp,y.timeStamp),x.value + y.value))
            .print()

//        设置延迟数据 tag
//        val lateText = new OutputTag[(String, String, Long, Long)]("late_data")

        env.execute("waterMarkTest")

    }
}
class MyPeriodicWaterMark extends AssignerWithPeriodicWatermarks[MyEvent]{ // 周期性生成 watermark 默认200ms生成一条 插入数据流中
    var currentTimestamp:Long = 0L
    val maxOutOfOrderness = 3000L
    var currentWaterMark = 0L

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    override def getCurrentWatermark: Watermark = { // be called per 200ms
        currentWaterMark = currentTimestamp - maxOutOfOrderness
//        println("CurrentWaterMark:"+currentWaterMark +" ["+sdf.format(currentWaterMark)+"] ")
        new Watermark(currentWaterMark)
    }

    override def extractTimestamp(element: MyEvent, previousElementTimestamp: Long): Long = {

        val timeStamp: Long = element.timeStamp
        currentTimestamp = Math.max(currentTimestamp, timeStamp)
        println("Key:" + element.name, "eventTime:" + timeStamp + decorate(sdf.format(timeStamp)) ," currentWater:"+currentWaterMark + decorate(sdf.format(currentWaterMark)))


        timeStamp
    }
    def decorate(content:String):String = " ["+content+"]"
}
class MyPunctuatedWaterMark extends AssignerWithPunctuatedWatermarks[MyEvent]{
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var currentTimestamp:Long = 0L
    val maxOutOfOrderness = 3000L
    var currentWaterMark = 0L
    override def checkAndGetNextWatermark(lastElement: MyEvent, extractedTimestamp: Long): Watermark = { // 每一条数据 按照其eventsTime 生成并携带一个水位线
        currentWaterMark = currentTimestamp - maxOutOfOrderness
        println("get waterMark :"+ System.currentTimeMillis())
        new Watermark(currentWaterMark)
    }

    override def extractTimestamp(element: MyEvent, previousElementTimestamp: Long): Long = {

        val timeStamp: Long = element.timeStamp
        currentTimestamp = Math.max(currentTimestamp, timeStamp)
        println("Key:" + element.name,"value:"+element.value,"eventTime:" + timeStamp + decorate(sdf.format(timeStamp)) ," currentWater:"+currentWaterMark + decorate(sdf.format(currentWaterMark)))

        timeStamp
    }
    def decorate(content:String):String = " ["+content+"]"
}
case class MyEvent(name:String,timeStamp:Long,value:Int)