package com.yowaqu.algorithm

import java.io.FileWriter

/**
  * @ ObjectName CreateRandomNum
  * @ Author fibonacci
  * @ Description create random number to be sort
  * @ Date 2019/9/19
  * @ Version 1.0
  */
import scala.util.Random
object CreateRandomNum {
    def main(args: Array[String]): Unit = {
        val location:String = this.getClass.getResource("").getPath.replace(".","/")
//            this.getClass.getPackage.getName.toString.replace(".","/")
        val writer = new FileWriter(s"${location}/Random6b.txt")
        for(i<-1 to 10 * 10000)
            writer.write(Random.nextLong().toString+"\n")
        writer.close()
    }
}
