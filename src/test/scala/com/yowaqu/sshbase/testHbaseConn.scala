package com.yowaqu.sshbase

import org.apache.hadoop.hbase.client.Result

/**
  * @ObjectName testHbaseConn 
  * @Author fibonacci
  * @Description TODO
  * @Date 19-7-18
  * @Version 1.0
  */
object testHbaseConn {
    def main(args: Array[String]): Unit = {
        val hbaseUtils = new HbaseUtils("127.0.0.1","2181")
//        hbaseUtils.scanTable("city")
//        hbaseUtils.querySingleRow("city","472","colFmly",Array("city_cd","city_nm")) // 不好用
//        println(hbaseUtils.isAvailable("city"))
//        hbaseUtils.getRowMultiVersion("city","472","colFmly",Array("city_cd","city_nm","location"))

//        println(hbaseUtils.getRowMap("city","022","colFmly"))
//        println(hbaseUtils.getRowMap("city","024","colFmly",Array("city_nm")))
//          .foreach(println)
//        hbaseUtils.createTable("city","colFmly")
        hbaseUtils.insertOneRow("city","010","colFmly",Map("city_nm"->"北京"))
    }

}
