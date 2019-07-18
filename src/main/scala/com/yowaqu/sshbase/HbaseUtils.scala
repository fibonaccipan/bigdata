package com.yowaqu.sshbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, TableDescriptorBuilder}

/**
  * @ObjectName HbaseUtils 
  * @Author fibonacci
  * @Description TODO
  * @Date 19-7-17
  * @Version 1.0
  */
object HbaseUtils {

    /**
      *设置HbaseConfiguration
      * @param quorum
      * @param port
      * @param tableName
      */
    def getHbaseConfig(quorum:String,port:String,tableName:String):Configuration ={
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum",quorum)
        conf.set("hbase.zookeeper.property.clientPort",port)

        conf
    }

    /**
      * 获取或新建HbaseAdmin
      */
    def getHbaseAdmin(conf:Configuration,tableName:String):HBaseAdmin ={
        val admin = new HBaseAdmin(conf)
        if(admin.isTableAvailable(TableName.valueOf(tableName))){
            val tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
            admin.createTable(tableDescriptor.build())
        }
        admin
    }

    /**
      *
      */

}
