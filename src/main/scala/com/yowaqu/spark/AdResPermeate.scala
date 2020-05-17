package com.yowaqu.spark

/**
  * @ ClassName AdResPermeate 
  * @ Author fibonacci
  * @ Description TODO
  * @ Date 2019/9/29
  * @ Version 1.0
  */
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.SparkSession

object AdResPermeate {
    val LOGGER = LogFactory.getLog(AdResPermeate.getClass)

    def getEarliestDate(spark:SparkSession):String={
        val s = "dd"
        s
    }

}
