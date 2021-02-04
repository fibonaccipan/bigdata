/**
 * @ Author panxianzhao
 * @ date 2020/11/12
 * @ Subject TODO
 * @ Version 1.0
 * @ Description TODO
 * @ Param_list TODO
 */
package com.yowaqu.scala

class Table {
    private var privatelength = 0

    def length = privatelength
    def length_= (x:Int) {
        privatelength = x
    }
    println("xxx")
}

object Box{
    def main(args: Array[String]): Unit = {
        val t1 = new Table()
        //        t1.length = 1
        t1.length = 9
        println(t1.length)
    }
    val a = Seq("sss")
}
