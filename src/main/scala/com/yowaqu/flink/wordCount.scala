package com.yowaqu.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object wordCount {
    def main(args: Array[String]): Unit = {
        //创建批处理执行环境
        val env = ExecutionEnvironment.getExecutionEnvironment

        // 读取文件的数据
        val inputDataset = env.readTextFile("src/main/scala/com/yowaqu/resources/exampleWord.txt")
        val word = inputDataset.flatMap(_.split(" "))
          .map((_,1))

        // 分组聚合
        val rslt = word.groupBy(0).sum(1)
        rslt.print()


    }
}
