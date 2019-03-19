package com.Tag

import org.apache.spark.sql.Row

/**
  * 设备标签
  */
object TagsDevice extends Tags {
  /**
    * 打标签的接口
    *
    */
  override def MakeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    // 操作系统
    val client = row.getAs[Int]("client")
    client match {
      case 1 => list:+=("D00010001",1)
      case 2 => list:+=("D00010002",1)
      case 3 => list:+=("D00010003",1)
      case _ => list:+=("D00010004",1)
    }
    // 联网方式
    val network = row.getAs[String]("networkmannername")
    network match {
      case "Wifi" => list:+=("D00020001",1)
      case "4G" => list :+=("D00020002",1)
      case "3G" => list :+=("D00020003",1)
      case "2G" => list :+=("D00020004",1)
      case _ => list :+=("D00020005",1)
    }
    // 运营商
    val ispname = row.getAs[String]("ispname")
    ispname match {
      case "移动" => list:+=("D00030001",1)
      case "联通" => list:+=("D00030002",1)
      case "电信" => list:+=("D00030003",1)
      case _ => list:+=("D00030004",1)
    }
    list
  }
}
