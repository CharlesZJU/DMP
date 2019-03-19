package com.Tag

import ch.hsr.geohash.GeoHash
import com.Utils.{App2Jedis, BaiduLBSHandler}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 通过百度的逆地址解析服务，进行编码解析，得到商圈
  */
object LongAndLatAndBD {
  def main(args: Array[String]): Unit = {
    if(args.length!=1){
      sys.exit()
    }
    val Array(inputPath) =args
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
      // 默认是java序列化方式，我们需要改成scala序列化方式，这样可以提高效率
      // 因为scala的序列化方式比java的序列化方式体积小，速度快，要比java快10倍
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val df = sqlContext.read.parquet(inputPath)
    df.select("lat","long").filter(
      """
        |cast(long as double) >= 73 and cast(long as double)<=136 and
        |cast(lat as double) >=3 and cast(lat as double) <= 54
      """.stripMargin).distinct()
      .foreachPartition(t=>{
        val jedis = App2Jedis.getConnection()
        t.foreach(t=>{
          // 取值经纬度
          val long = t.getAs[String]("long")
          val lat = t.getAs[String]("lat")
          // 将通过百度解析商圈信息
          val business = BaiduLBSHandler.parseBusinessTagBy(long,lat)
          // 将经纬度转换geohash编码
          val geoHashs = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,8)
          // 存入redis中
          jedis.set(geoHashs,business)
        })
        jedis.close()
      })
  }
}
