package com.Rpt

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计各省市的数据量
  */
object ProCityRpt {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("退出程序")
      sys.exit()
    }
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val Array(inputPath,outputPath) = args
    // 获取数据源
    val df = sqlContext.read.parquet(inputPath)
    df.registerTempTable("log")
    // 处理业务数据
    val result = sqlContext.sql("select provincename,cityname,count(*) cts from log group by provincename,cityname")
    result.coalesce(1).write.json(outputPath)

    sc.stop()
  }
}
