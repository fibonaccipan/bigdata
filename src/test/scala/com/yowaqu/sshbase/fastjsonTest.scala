package com.yowaqu.sshbase

import com.alibaba.fastjson.JSON

/**
  * @ObjectName fastjsonTest 
  * @Author fibonacci
  * @Description TODO
  * @Date 19-7-27
  * @Version 1.0
  */
object fastjsonTest {
    def main(args: Array[String]): Unit = {
        val o = new Order()
        o.setArea_code("10001")
        o.setCity_name("南京市")
        println(o)
        println(JSON.toJSONString(o,false))
    }
}
