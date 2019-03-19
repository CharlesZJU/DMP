package com.Tag

import ch.hsr.geohash.GeoHash
import com.Utils.App2Jedis
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object text {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
      // 默认是java序列化方式，我们需要改成scala序列化方式，这样可以提高效率
      // 因为scala的序列化方式比java的序列化方式体积小，速度快，要比java快10倍
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    val df = sqlContext.read.parquet("hdfs://node1:9000/DMP")

    df.filter(
      """
        |cast(long as double) >= 73 and cast(long as double)<=136 and
        |cast(lat as double) >=3 and cast(lat as double) <= 54
      """.stripMargin).map(row=>{
      val jedis = App2Jedis.getConnection()
        val lat = row.getAs[String]("lat")
        val long = row.getAs[String]("long")
        // 转换geoHash码
        val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,8)
        // 在redis中取值
        val str = jedis.get(geoHash)
        str
    }).foreach(println)

  }
}
