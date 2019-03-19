package com.Tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object TagsAPP extends Tags {
  /**
    * 打标签的接口
    *
    */
  override def MakeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 将参数转换类型
    val row = args(0).asInstanceOf[Row]
    val appdir = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    // 获取appname  appid
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if(StringUtils.isNotBlank(appname)){
      list :+=("APP"+appname,1)
    }else if(StringUtils.isNotBlank(appid)){
      list :+=("APP"+appdir.value.getOrElse(appid,appid),1)
    }
    list
  }
}
