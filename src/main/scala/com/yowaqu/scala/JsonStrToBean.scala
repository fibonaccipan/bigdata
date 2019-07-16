package com.yowaqu.scala
import org.json4s._
import org.json4s.jackson.JsonMethods._

class Stu(val id: String,val name:String)
object JsonStrToBean {
  def main(args: Array[String]): Unit = {
    val jsonStr:String = "{\"id\":\"12345\",\"name\":\"fibonacci\"}"
    implicit val formats = DefaultFormats
    println(jsonStr)
    val x = parse(jsonStr).extract[Stu]
    println(x)
    println(x.id)
    println(x.name)
  }
}

