/**
 * @ Author panxianzhao
 * @ date 2021/2/4
 * @ Subject TODO
 * @ Version 1.0
 * @ Description TODO
 * @ Param_list TODO
 */
package com.yowaqu.scala

object exec {
    def main(args: Array[String]): Unit = {
        val f1 = fraction(1,2) * fraction(1,3)
        val f2 = fraction(1,2) + fraction(1,3)
        println("f1 is :"+f1.n +" / "+f1.m+" and value is "+f1.value)
        println("f2 is :"+f2.n +" / "+f2.m+" and value is "+f2.value)
        val fraction(x,y) = f1 + f2 // 调用unapply 提取器初始化 x y
        val fraction(z,w) = f1 * f2
//        var fraction(x,y) = fraction(2,4) * fraction(3,4)
//
        println("x is: " + x + " and y is: " + y)
        println("z is: " + z + " and w is: " + w)
//        println()


    }
}
