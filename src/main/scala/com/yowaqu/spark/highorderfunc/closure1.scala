package com.yowaqu.spark.highorderfunc

object closure1 {
    def main(args: Array[String]): Unit = {
        val arr = Array[String]("hello","world","meet","you","soon")
        arr.foreach(println)
        println("="* 30)

        // try Function as a parameter
        val echo1 = (value:String) => println("this word is :"+value)
        arr.foreach(echo1)
        println("="* 30)

        // try CLOSURES to give a specific Function
        def echoTimes(time:Int) = (value:String)=> (println(value * time))

        val norepeat = echoTimes(1)
        val doublePrint = echoTimes(2)
        arr.foreach(norepeat)
        arr.foreach(doublePrint)
        println("="* 30)

        arr.foreach(x => echoTimes(x.length)(x))

    }
}
