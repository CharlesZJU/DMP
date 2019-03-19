package com.parquet

import com.Utils.{DFUtils, SchemaUtils}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将数据读取转换成parquet文件格式
  * 要求一： 将数据转换成 parquet 文件格式
  * 要求二： 序列化方式采用 KryoSerializer 方式
  * 要求三： parquet 文件采用 Snappy 压缩方式
  */
object Bz2Parquetv2 {
  def main(args: Array[String]): Unit = {
    // 模拟企业编程 首先判断目录是否为空
    if(args.length != 2){
       println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个数组存储输入输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
      // 默认是java序列化方式，我们需要改成scala序列化方式，这样可以提高效率
      // 因为scala的序列化方式比java的序列化方式体积小，速度快，要比java快10倍
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 读取数据
    val lines = sc.textFile(inputPath)
    //val lines = sc.parallelize(Array("0bb49045000057eee4ed3a580019ca06,0,0,0,100002,未知,26C07B8C83DB4B6197CEB80D53B3F5DA,1,1,0,0,2016-10-01 06:19:17,139.227.161.115,com.apptreehot.horse,马上赚钱,AQ+KIQeBhehxf6x988FFnl+CV00p,A10%E5%8F%8C%E6%A0%B8,1,4.1.1,,768,980,,,上海市,上海市,4,未知,3,Wifi,0,0,2,插屏,1,2,6,未知,1,0,0,0,0,0,0,0,,,,,,,,,,,,0,555,240,290,,,,,,,,,,,AQ+KIQeBhehxf6x988FFnl+CV00p,,1,0,0,0,0,0,,,mm_26632353_8068780_27326559,2016-10-01 06:19:17,,"))
    // 进行数据过滤，保证字段大于八十五个 如果数据内部有多个连续一样的,,,,有些无法解析，需要精确数据
    val rowRDD = lines.map(t=>t.split(",",t.length)).filter(_.length >= 85).map(t=>Log(t))

    val df = sqlContext.createDataFrame(rowRDD)
    df.write.parquet(outputPath)
    sc.stop()
  }
}
