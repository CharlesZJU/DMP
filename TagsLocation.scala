package com.Tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object TagsLocation extends Tags {
  /**
    * 打标签的接口
    *
    */
  override def MakeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    // 取到对应的字段
    val pro = row.getAs[String]("provincename")
    val city = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank(pro)){
      list:+=("ZP"+pro,1)
    }
    if(StringUtils.isNotBlank(city)){
      list:+=("ZC"+city,1)
    }
    list
  }
}
