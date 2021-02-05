/**
 * @ Author panxianzhao
 * @ date 2021/2/4
 * @ Subject : scala collection related prove
 * @ Version 1.0
 * @ Description TODO
 * @
 *             |----> Seq ----> IndexSeq
 *             |
 * Iterable ---|----> Set ----> SortedSeq
 *             |
 *             |----> Map ----> SortedMap
 *
 * range 对象不存储值，只存开始、结束、步长
 * vector 对象可以有32子节点，随机访问快
 *
 */
package com.yowaqu.scala.collection

object exec {
    def main(args: Array[String]): Unit = {
        // 1 compare two iterable
        val a:Boolean = Seq(1,2,3) == (1 to 3)
        println(a)
        println(Seq(1,3,4,5).getClass +"\n"+ (1 to 5).getClass)
        val map = Map('a'->1,'b'->2)
        val map2 = scala.collection.Map('a'->1,'b'->3)
        println(map +"-->>"+ map.getClass)
        println(map2 +"-->>"+ map2.getClass)
        val s = IndexedSeq(1)
        val list:List[String] = Nil

    }
}
