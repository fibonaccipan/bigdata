package com.yowaqu.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession



/**
  * @ Object Name Demo1
  * @ Author fibonacci
  * @ Description TODO
  * @ Date 2019/9/27
  * @ Version 1.0
  */

object Demo1 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
          .setMaster("local[*]")
          .setAppName("sparkDemo1")

//        val sparkSession = SparkSession
        val xxx = "sss"
        println()

    }
}
