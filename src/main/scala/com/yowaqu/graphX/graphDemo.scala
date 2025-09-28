package com.yowaqu.graphX

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
object graphDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("graphX test demo1")
      .master("local[*]") // 在本地运行，使用所有核心
      .getOrCreate()
    val sc = spark.sparkContext

    // 1. 创建顶点 RDD
    // 格式: (VertexId, 属性)
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array(
      (1L, ("Alice", "Developer")),
      (2L, ("Bob", "Manager")),
      (3L, ("Charlie", "Engineer")),
      (4L, ("David", "Intern"))
    ))

    // 2. 创建边 RDD
    // 格式: Edge(源VertexId, 目标VertexId, 属性)
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(1L, 2L, "follows"),
      Edge(1L, 3L, "follows"),
      Edge(2L, 3L, "supervises"),
      Edge(3L, 4L, "mentors")

    ))
    // 3. 定义一个默认顶点，用于处理图中存在边但不存在对应顶点的情况
    val defaultUser = ("Unknown", "Missing")

    // 4. 构建图
    val graph = Graph(users, relationships, defaultUser)

    // 打印顶点和边
    println("Vertices:")
    graph.vertices.collect.foreach(println)
    println("Edges:")
    graph.edges.collect.foreach(println)
    println("Triplets")
    graph.triplets.collect.foreach(triplet =>
      println(s"${triplet.srcAttr._1} (${triplet.srcAttr._2}) -- ${triplet.attr} --> ${triplet.dstAttr._1} (${triplet.dstAttr._2})"))
    val newGraph = graph.mapVertices((id,attr)=>attr._1.toUpperCase)
    newGraph.vertices.collect.foreach(println)
    // 如何提取子图 graph.subGraph
    //图反转graph.reverse
    println("展示出度入度")
    graph.degrees.collect.foreach(println)
    graph.outDegrees.collect.foreach(println)
  }
}
