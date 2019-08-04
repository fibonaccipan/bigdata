package com.yowaqu.sshbase

import com.alibaba.fastjson.JSON

import com.alibaba.fastjson.JSONObject

/**
  * @ClassName MsgHandler 
  * @Author fibonacci
  * @Description TODO
  * @Date 19-7-16
  * @Version 1.0
  */
class MsgHandler extends Serializable {
    /**
      * check JSON format
      * @param jsonStr
      * @return
      */
    private def checkJsonFormat(jsonStr:String): Boolean ={
       if(jsonStr != null){
           try{
               val jsonObject = JSON.parseObject(jsonStr)
               true
           } catch {
               case e:Exception => e.printStackTrace()
                   println("json 格式异常")
                   false
           }
       } else {
           false
       }
    }

    /**
      * check json contains necessary key
      * @param jsonObject
      * @param fieldsList
      *
      */

    private def checkJsonKey(jsonObject:JSONObject,fieldsList:Array[String]): Boolean ={
        if(jsonObject != null && fieldsList.length > 0 ){
            //java 写法
         /* for(field <- fieldsList ){
               if(!jsonObject.containsKey(field))
                    false
            }
            true
          */
            // scala 简化写法
//            fieldsList.reduce(jsonObject.containsKey(_) && jsonObject.containsKey(_))
            fieldsList.map(jsonObject.containsKey(_)).reduce(_ && _)
        }else{
            false
        }
    }

    /**
      * check json contains necessary value
      * @param jsonObject
      * @param fieldsList
      */
    private def checkJsonValue(jsonObject: JSONObject,fieldsList:Array[String]): Boolean ={
        if(jsonObject != null && fieldsList.length >0){
            fieldsList.map(jsonObject.get(_).toString).map(ifInvaildValue).reduce(_ && _)
//            fieldsList.reduce((x,y) => ifInvaildValue(jsonObject.get(x).toString) && ifInvaildValue(jsonObject.get(y).toString))
        }else{
            false
        }
    }

    /**
      * check json's all basic info
      * @param jsonStr
      */
    private def checkAll(jsonStr:String,configManager:ConfigManager): Boolean ={
        if(checkJsonFormat(jsonStr)){
            val jsonObject:JSONObject = JSON.parseObject(jsonStr)
            checkJsonKey(jsonObject,configManager.getProperty("must.fields.key").split(",")) &&
            checkJsonValue(jsonObject,configManager.getProperty("must.fields.value").split(","))
        }else{
            false
        }
    }

    /**
      * filter pay amount if >= 100 return true
      * @param payAmnt
      */
    private def filterPayAmnt(payAmnt:String):Boolean={
        payAmnt.toDouble >= 20
//        true
    }

    /**
      * filter Msg when jsonObject.pay_amnt>=100 return true
      * @param jsonObject
      */
    private def filterMsg(jsonObject:JSONObject): Boolean ={
        if(jsonObject!=null){
            filterPayAmnt(jsonObject.getString("pay_amount"))
        }else{
            false
        }
    }

    /**
      * @param jsonStr
      * @param configManager
      */
    def handlerMsg(jsonStr:String,configManager: ConfigManager):Boolean={
        if(jsonStr != null && configManager != null){
            if(checkAll(jsonStr,configManager)){
                val jsonObject = JSON.parseObject(jsonStr)
                filterMsg(jsonObject)
            }else{
                false
            }
        }else{
            false
        }
    }

    /**
      *@param v value to be judge
      */
    private def ifInvaildValue(v:String):Boolean={
       !{v == null || v.equals("") || v =="-"} // 若值 符合条件 则为无效值 返回 false
    }

    /**
      *
      */
    def getOrderBean(jsonStr:String): Order ={
        if(jsonStr != null){
            try{
                val order:Order = JSON.parseObject(jsonStr,classOf[Order])
                order
            }catch{
                case e:Exception => e.printStackTrace()
                    println("转换OrderBean失败")
                    null
            }
        }else{
            null
        }

    }
}
