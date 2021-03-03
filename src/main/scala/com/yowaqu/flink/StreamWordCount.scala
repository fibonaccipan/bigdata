package com.yowaqu.flink

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
    def main(args: Array[String]): Unit = {
        // 创建流处理环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        // 创建socket 数据流
        val textDataStream = env.socketTextStream("127.0.0.1",7778)
        val wordCountStream = textDataStream.flatMap(_.split("\\s"))
          .filter(_.nonEmpty)
          .map((_,1))
          .keyBy(0)
          .sum(1)

        wordCountStream.print().setParallelism(2)

        //执行任务
        env.execute("StreamWordCount")

    }
}
