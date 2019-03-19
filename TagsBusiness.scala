package com.Tag

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * 商圈标签
  */
object TagsBusiness extends Tags {
  /**
    * 打标签的接口
    *
    */
  override def MakeTags(args: Any*): List[(String, Int)] = {
    val row =args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    var list =List[(String,Int)]()
      val lat = row.getAs[String]("lat")
      val long = row.getAs[String]("long")
      // 转换geoHash码
      val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,8)
      //println(geoHash)
      // 在redis中取值
      val str = jedis.get(geoHash)
      list:+=(str,1)
    //list.foreach(println)
    list
  }
}
