package com.yowaqu.sshbase

import java.io.Serializable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellScanner, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

/**
  * @ObjectName HbaseUtils 
  * @Author fibonacci
  * @Description TODO
  * @Date 19-7-17
  * @Version 1.0

  * according http://blog.itpub.net/31506529/viewspace-2214159
  */
class HbaseUtils(quorum:String,port:String) extends Serializable {

    private var configuration:Configuration = _

    private var conn:Connection = _

    implicit def MMap2Map(map:mutable.Map[String,String]):Map[String,String]=map.toMap
    setHbaseConfig(quorum,port)

    /**
      *设置HbaseConfiguration && HBaseConnection
      * @param quorum
      * @param port
      */
    private def setHbaseConfig(quorum:String,port:String):Unit={
        configuration = HBaseConfiguration.create()
        configuration.set("hbase.zookeeper.quorum",quorum)
        configuration.set("hbase.zookeeper.property.clientPort",port)
        try{
            this.conn = ConnectionFactory.createConnection(configuration)
        } catch {
            case e:Exception => e.printStackTrace()
                println("create HBase Connection failed !")
        }
    }

    /**
      * 测试建表功能
      */
    def createTable(tableName:String,colFamily:String): Unit ={
        val admin = conn.getAdmin
        if(!admin.isTableAvailable(TableName.valueOf(tableName))){
            val table = TableName.valueOf(tableName)
            val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table)
            val colFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFamily))
            val colFamilyDescriptor = colFamilyDescriptorBuilder.build()
            tableDescriptorBuilder.setColumnFamily(colFamilyDescriptor)
            val tableDescriptor = tableDescriptorBuilder.build()
            admin.createTable(tableDescriptor)
        }
    }

    /**
      * 插入
      */
    def insertOneRow(tableName:String,rowKey:String,colFamily:String,map:Map[String,String]): Unit ={
        val put = new Put(Bytes.toBytes(rowKey))
        map.foreach(x => put.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(x._1),Bytes.toBytes(x._2)))
        val table = conn.getTable(TableName.valueOf(tableName))
        table.put(put)
    }

    /**
      * 按照rowKey 查询某行数据
      */
    def querySingleRow(tableName:String,rowkey:String,colFamily:String,columns:Array[String]) ={
        try{
            val table:Table =conn.getTable(TableName.valueOf(tableName))
            val get:Get = new Get(Bytes.toBytes(rowkey))
            val rslt = table.get(get).rawCells().iterator
            while(rslt.hasNext){
                val cell = rslt.next()
                println(Bytes.toString(cell.getQualifierArray,cell.getQualifierOffset,cell.getQualifierLength)+
                    Bytes.toString(cell.getValueArray,cell.getValueOffset,cell.getValueLength))
                println(Bytes.toString(cell.getQualifierArray))
            }
        } catch {
            case e:Exception => e.printStackTrace()
//                println("aaaa")
                null
        }
    }

    /**
      * 判断表是否可用
      */
    def isAvailable(tableName:String): Boolean ={
        val admin:Admin = this.conn.getAdmin
        admin.isTableAvailable(TableName.valueOf(tableName))
    }

    /**
      * 全表扫描
      */
    def scanTable(tableName:String) ={
        val table:Table = conn.getTable(TableName.valueOf(tableName))
        val scan:Scan = new Scan()
        val rsltScn:ResultScanner = table.getScanner(scan)
        val rslt = rsltScn.iterator()
        while(rslt.hasNext){
            val rs = rslt.next()
            val rowkey = Bytes.toString(rs.getRow)
            println("row key :"+rowkey)
            val cells = rs.rawCells()
            cells.foreach(cell => {
                println(Bytes.toString(cell.getQualifierArray,cell.getQualifierOffset,cell.getQualifierLength)
                ,Bytes.toString(cell.getValueArray,cell.getValueOffset,cell.getValueLength))
            })
            println("-----------------")
        }
    }
    /**
      *  查询指定列的多个版本数据
      */
    def getRowMultiVersion(tableName:String,rowkey:String,colFamily:String,columns:Array[String]): Unit ={
        val table:Table = conn.getTable(TableName.valueOf(tableName))
        val get = new Get(Bytes.toBytes(rowkey))
        get.readVersions(1)
        val f1 = (x:String)=>get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(x))
        columns.foreach(f1)

//        get.addFamily(Bytes.toBytes(colFamily))
        val result = table.get(get)
        val cellScanner:CellScanner = result.cellScanner()
        while(cellScanner.advance()){
            val cell = cellScanner.current()
            println("rowKey: " + Bytes.toString(CellUtil.copyRow(cell)))
            println("columnFamily: " + Bytes.toString(CellUtil.cloneFamily(cell)))
            println("column: " + Bytes.toString(CellUtil.cloneQualifier(cell)))
            println("value: " + Bytes.toString(CellUtil.cloneValue(cell)))
        }
        /*val colCellScanner = result.getColumnCells(Bytes.toBytes("INFO"),Bytes.toBytes("city_nm"))
          .iterator()
        while(colCellScanner.hasNext){
            val cell = colCellScanner.next()
            println(Bytes.toString(cell.getValueArray))
            println(Bytes.toString(CellUtil.cloneValue(cell)))
        }*/


        println("11111111111111111")
        table.close()
    }

    /**
      *  查询指定列族 并转为 map
      */
    def getRowMap(tableName:String,rowKey:String,colFamily:String):Map[String,String]={
        val rsltMap = mutable.Map[String,String]()
        val table:Table = conn.getTable(TableName.valueOf(tableName))
        val get:Get = new Get(Bytes.toBytes(rowKey))
        get.addFamily(Bytes.toBytes(colFamily))
        val result = table.get(get).cellScanner()
        fetchToMap(result)
    }

    /**
      *  查询制定列 并转为 map
      */
    def getRowMap(tableName:String,rowKey:String,colFamily:String,columns:Array[String]):Map[String,String]={
        val table:Table = conn.getTable(TableName.valueOf(tableName))
        val get:Get = new Get(Bytes.toBytes(rowKey))
        columns.foreach(x => get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(x)))
        val result = table.get(get)
//        fetchToMap(result.cellScanner()) //
        fetchToMap(result)
    }

    /**
      *  解析查询结果返回map
      *  @param cellScanner
      *  @return Map(col,value)[String,String]
      */
    def fetchToMap(cellScanner: CellScanner):Map[String,String] ={
        val rsltMap = mutable.Map[String,String]()
        while(cellScanner.advance()){
            val cell = cellScanner.current()
            val key = Bytes.toString(cell.getQualifierArray,cell.getQualifierOffset,cell.getQualifierLength)
            val value = Bytes.toString(cell.getValueArray,cell.getValueOffset,cell.getValueLength)
            rsltMap += (key -> value)
        }
        rsltMap
    }
}
