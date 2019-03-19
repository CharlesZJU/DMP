package com.Rpt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

object ProCityRptV2 {
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
    // 在resource这个配置文件中，优先级顺序执行
    // application.conf->application.json->application.properties
    val load = ConfigFactory.load()
    val pro = new Properties()
    pro.setProperty("user",load.getString("jdbc.user"))
    pro.setProperty("password",load.getString("jdbc.password"))
    // 将数据存入mysql中
    result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),pro)
  }
}
