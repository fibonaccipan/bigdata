package com.yowaqu.sshbase

import java.io.FileInputStream
import java.util.Properties

/**
  * @ClassName ConfigManager
  * @Author fibonacci
  * @Description TODO
  * @Date 19-7-16
  * @Version 1.0
  */

class ConfigManager(propName:String) extends Serializable {
    private val prop = new Properties()
    @transient
    private var in:FileInputStream = _
    try{
        in = new FileInputStream(s"src/main/scala/com/yowaqu/resources/$propName.properties")
        this.prop.load(in)
        println("读取配置文件成功")
    }catch {
        case e:Exception => e.printStackTrace();println("读取配置文件失败")
    }finally
        if(in != null)
            in.close()

    def getProperty(key:String):String={
        if(this.prop != null)
            this.prop.getProperty(key)
        else
            null
    }
}
