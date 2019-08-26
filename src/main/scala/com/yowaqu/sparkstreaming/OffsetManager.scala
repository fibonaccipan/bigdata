package com.yowaqu.sparkstreaming

import java.sql.{Connection, DriverManager}

import com.yowaqu.sshbase.ConfigManager
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange



/**
  * @ObjectName OffsetManager 
  * @Author fibonacci
  * @Description
  * @Date 19-8-25
  * @Version 1.0
  */
object OffsetManager {
    private val cfg = new ConfigManager("mysql")
    private val mysqlConf = Map[String,String](
        "driver"->"com.mysql.jdbc.Driver",
        "url"->s"jdbc:mysql://${cfg.getProperty("mysql.host")}:${cfg.getProperty("mysql.port")}/${cfg.getProperty("kafka.schema")}?autoReconnect=true",
        "username"->cfg.getProperty("mysql.username"),
        "password"->cfg.getProperty("mysql.password")
    )
    Class.forName(mysqlConf("driver"))
    private def getConn:Connection = DriverManager
      .getConnection(mysqlConf("url"),mysqlConf("username"),mysqlConf("password"))

    private def getStmt = getConn
      .prepareStatement("replace into offsets values (?,?,?,?)")

    private def preStmt = getConn
      .prepareStatement("select topic,groupid,prt,offset from offsets " +
        "where topic=? and groupid=?")
    def apply(topic:String,groupid:String) ={
        val stmt = preStmt
        stmt.setString(1,topic)
        stmt.setString(2,groupid)
        val rs = stmt.executeQuery()
        import scala.collection.mutable._
        val offsetRange = Map[TopicPartition,Long]()
        while(rs.next()){
            offsetRange += new TopicPartition(rs.getString("topic"),rs.getInt("prt")) -> rs.getLong("offset")
        }
        rs.close()
        stmt.close()
        offsetRange.toMap
    }

    def saveCurrentBatchOffset(groupid:String,OffsetRanges:Array[OffsetRange]): Unit ={
        val stmt = getStmt
        for (offsetRange <- OffsetRanges){
            stmt.setString(1,offsetRange.topic)
            stmt.setString(2,groupid)
            stmt.setInt(3,offsetRange.partition)
            stmt.setLong(4,offsetRange.untilOffset)
            stmt.executeUpdate()
        }
        stmt.close()
    }
}
