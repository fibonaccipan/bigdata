package com.yowaqu.tmp

import scala.io.Source
import org.json.JSONObject

object process1 {
  def main(args: Array[String]): Unit = {
    val file = Source.fromFile("/home/fibonacci/data/order.txt")
    var lines = file.getLines().toArray
    file.close()
    val jsonHeader:Array[String] = lines.head.split("\t")
    lines = lines.drop(1)
    val city = scala.collection.mutable.Map[String,String]()
    for(pos <- lines.indices if pos < 10000) {
      val tmpJson = new JSONObject()
      for(i <- jsonHeader.indices){
        tmpJson.put(jsonHeader(i),lines(pos).split("\t")(i))
      }
       city(tmpJson.get("city_code").asInstanceOf[String])= tmpJson.get("city_name").asInstanceOf[String]
    }
    (city -= "-")
      .foreach(x => println(s"put 'city','${x._1}','colFmly:city_cd','${x._1}'\nput 'city','${x._1}','colFmly:city_nm','${x._2}'"))
  }
}
