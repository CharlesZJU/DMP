package com.APP

import com.Utils.{App2Jedis, RptUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer

/**
  * 媒体指标（Redis）
  */
object AppRedis {
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
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 读取数据源
    val df = sqlContext.read.parquet(inputPath)
    df.mapPartitions(row=>{
      // 获取连接
     val jedis = App2Jedis.getConnection()
      val list = ListBuffer[(String,List[Double])]()
      row.foreach(row=>{
        // 处理APP名称
        var name = row.getAs[String]("appname")
        if(!StringUtils.isNotBlank(name)){
          name = jedis.get(row.getAs[String]("appid"))
        }
        // 先获取 原始请求，有效请求，广告请求
        val requestmode = row.getAs[Int]("requestmode")
        val processnode = row.getAs[Int]("processnode")
        // 参与竞价数，成功数，展示数  点击数
        val iseffective = row.getAs[Int]("iseffective")
        val isbilling = row.getAs[Int]("isbilling")
        val isbid = row.getAs[Int]("isbid")
        val iswin = row.getAs[Int]("iswin")
        val adorderid = row.getAs[Int]("adorderid")
        val winprice = row.getAs[Double]("winprice")
        val ad = row.getAs[Double]("adpayment")
        // 编写业务方法，进行调用 原始请求，有效请求，广告请求
        val reqlist = RptUtils.req(requestmode,processnode)
        // 参与竞价数，成功数，展示数  点击数
        val adlist = RptUtils.addap(iseffective,isbilling,isbid,iswin,adorderid,winprice,ad)
        // 点击数 展示数
        val adCountlist = RptUtils.Counts(requestmode,iseffective)
        list += ((name,reqlist ++ adlist ++ adCountlist))
      })
      jedis.close()
      list.iterator
    }).reduceByKey((list1,list2)=>{
      // list1(0,1,1,0) list2(1,1,1,1) zip((0,1),(1,1),(1,1),(0,1))
      list1.zip(list2).map(t=>t._1+t._2)
    })// 调整下方位
      .map(t=>t._1+" , "+t._2.mkString(","))
      // 将结果数据存入hdfs
      .saveAsTextFile(outputPath)

  }
}
