package com.yowaqu.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @ObjectName WatermarkTest 
  * @Author fibonacci
  * @Description TODO
  * @Date 19-8-17
  * @Version 1.0
  */
object WatermarkTest {
    def main(args: Array[String]): Unit = {
        val hostName = "localhost"
        val port:Int = 10050
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val input = env.socketTextStream(hostName,port)
//        val f = (line:String) =>{val arr = line.split("\\W+");(arr(0),arr(1))}
//        val inputMap = input.map(f=>{
//            val arr=f.split("\\W+")
//            val code = arr(0)
//            val time = arr(1).toLong
//            (code,time)
//        })
    }
}
