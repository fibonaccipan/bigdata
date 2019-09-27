package com.yowaqu.algorithm

import java.io.FileWriter

import scala.io.Source

/**
  * @ ObjectName SortAlgorithm 
  * @ Author fibonacci
  * @ Description: Sort Algorithm example
  * @ Date 2019/9/19
  * @ Version 1.0
  * @ Source : https://www.cnblogs.com/onepixel/articles/7674659.html
  */
/**
  * 排序算法分类
  * 1.  排序算法
  *   1.1   比较排序算法
  *      1.1.1  交换排序
  *         1.1.1.1     冒泡排序
  *         1.1.1.2     快速排序
  *      1.1.2  插入排序
  *         1.1.2.1     简单插入排序
  *         1.1.2.2     希尔排序
  *      1.1.3  选择排序
  *         1.1.3.1     简单选择排序
  *         1.1.3.2     堆排序
  *      1.1.4  归并排序
  *         1.1.4.1     二路归并排序
  *         1.1.4.2     多路归并排序
  *   1.2   非比较排序算法
  *      1.2.1  计数排序
  *      1.2.2  桶排序
  *      1.2.3  基数排序
  * */
object SortAlgorithm{
    def swap(arr:Array[Long],pos1:Int,pos2:Int): Unit ={
        var tmp:Long = 0L
        tmp = arr(pos1)
        arr(pos1)= arr(pos2)
        arr(pos2) = tmp
    }

    def readFile(fileName:String):Array[Long]={
        val path = this.getClass.getResource("").getPath.replace(".","/")+fileName
        val file = Source.fromFile(path)
        val arr:Array[Long] = file
          .getLines
          .toArray
          .map(_.toLong)
//          .slice(0,1000)
        file.close()
        arr
    }
    def saveFile(arr:Array[Long]): Unit ={
        val out = new FileWriter(this.getClass.getResource("").getPath.replace(".","/")+"Random.txt")
        arr.foreach(x => out.write(x.toString+"\n"))
        out.close()
    }
    def bubbleSort(fileName:String): Unit ={
        val arr = readFile(fileName)
        //使用scala的 双层for循环
        for(i <- arr.indices;j <- 0 until arr.length-1-i)
            // i 代表 冒完的泡 次数，主要是J从数组头到尾 J和J+1 大的往上冒泡
            if(arr(j) > arr(j+1)) swap(arr,j,j+1)
        saveFile(arr)
    }
    // 快速排序 主要是依赖于 Core递归调用
    def quickSortCore(arr:Array[Long],start:Int,end:Int): Unit ={
        var i = start
        var j = end
        val target = arr(start)
        while(i<j){
            while(i<j && arr(j) >= target) j -= 1
            while(i<j && arr(i) <= target) i += 1
            if(i<j) swap(arr,i,j)
            if(i==j) swap(arr,start,i)
        }
        if(i-1 > start) quickSortCore(arr,start,i-1)
        if(j+1 < end) quickSortCore(arr,j+1,end)
    }
    def quickSort(fileName: String): Unit ={
        val arr = readFile(fileName)
        quickSortCore(arr,0,arr.length-1)
        saveFile(arr)
    }

    def main(args: Array[String]): Unit = {
        val start_time = System.currentTimeMillis()
//        bubbleSort("Random6b.txt") // 17秒
        quickSort("Random8b.txt") //16秒
        val delta = System.currentTimeMillis() - start_time
        println(s"本次排序共耗时：${delta} 微秒!")
    }
}
