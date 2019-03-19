package com.GraphX

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 图计算案例
  */
object gx {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
      // 默认是java序列化方式，我们需要改成scala序列化方式，这样可以提高效率
      // 因为scala的序列化方式比java的序列化方式体积小，速度快，要比java快10倍
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    // 构造出点的集合
    val seqRDD = sc.makeRDD(Seq(
      (1L,("李连杰",58)),
      (2L,("成龙",62)),
      (6L,("老詹",34)),
      (9L,("杜老二",30)),
      (133L,("库里",30)),
      (138L,("罗斯",32)),
      (16L,("保罗",34)),
      (44L,("法尔考",33)),
      (21L,("库蒂尼奥",36)),
      (158L,("马塞洛",32)),
      (5L,("内马尔",28)),
      (7L,("苏亚雷斯",34))
    ))
    // 构造边的集合
   val edgeRDD =  sc.makeRDD(Seq(
      Edge(1L,133L,0),
      Edge(2L,133L,0),
      Edge(9L,133L,0),
      Edge(6L,133L,0),
      Edge(6L,138L,0),
      Edge(21L,138L,0),
      Edge(16L,138L,0),
      Edge(44L,138L,0),
      Edge(5L,158L,0),
      Edge(7L,158L,0)
    ))
    // 构造图
    val graph: Graph[(String, Int), Int] = Graph(seqRDD,edgeRDD)
    val conn = graph.connectedComponents().vertices
    conn.join(seqRDD).map{
      case (userid,(vid,(name,age))) =>(vid,List(name,age))
    }
      .reduceByKey(_++_)
      .foreach(println)
  }
}
