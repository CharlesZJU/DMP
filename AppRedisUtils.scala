package com.Utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 先将数据存入redis
  */
object AppRedisUtils {
  def main(args: Array[String]): Unit = {
    // 模拟企业编程 首先判断目录是否为空
    if(args.length != 2){
      println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个数组存储输入输出目录
    val Array(inputPath,outputPath,dirPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
      // 默认是java序列化方式，我们需要改成scala序列化方式，这样可以提高效率
      // 因为scala的序列化方式比java的序列化方式体积小，速度快，要比java快10倍
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    // 将数据存入redis
    sc.textFile(dirPath).map(_.split("\t",-1))
      .filter(_.length >= 5).map(arr=>(arr(4),arr(1)))
      // 将字典文件存入redis
      .foreachPartition(ite=>{
      val jedis = App2Jedis.getConnection()
      ite.foreach(t=>{
        jedis.set(t._1,t._2)
      })
      jedis.close()
    })
    sc.stop()
  }
}
