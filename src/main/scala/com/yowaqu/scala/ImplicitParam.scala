package com.yowaqu.scala


object ImplicitParam {
  def main(args: Array[String]): Unit = {
    println("*" * 10)
    // 声明一个隐式的 引用 并指向 实例
    implicit val outputFormat = new OutputFormat("<<",">>")
    //调用该对象的使用了隐式参数的方法，默认去寻找能够匹配的对象隐式的传入。
    println(new Athlete("Ronaldinho").fromatAthleteName())
  }
}

class Athlete(var name:String){ //定义运动员类
  //类方法 格式化运动员姓名
  //使用 柯里化的方式 定义一个具有隐式形参的方法
  def fromatAthleteName()(implicit outputFormat: OutputFormat): String ={
    outputFormat.first + " " + name + " " + outputFormat.second
  }
}
class OutputFormat(val first:String,val second:String)