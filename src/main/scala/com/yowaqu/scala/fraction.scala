/**
 * @ Author panxianzhao
 * @ date 2021/2/4
 * @ Subject TODO
 * @ Version 1.0
 * @ Description TODO
 * @ Param_list TODO
 */
package com.yowaqu.scala


// example to prove apply and unapply function ,
class fraction(val n:Int,val m:Int) {
    val value:Double = n/m.toDouble

    def *(other:fraction) ={
        fraction(n*other.n , m*other.m)
    }
    def +(other:fraction)={
        fraction(n*other.m+other.n*m ,m*other.m)
    }
}

object fraction{
    def apply(n: Int, m: Int): fraction = new fraction(n, m)

    // 提取器
    def unapply(arg: fraction): Option[(Int, Int)] =
        if(arg.m == 0) None else Some((arg.n,arg.m))
}
