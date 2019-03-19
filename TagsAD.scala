package com.Tag

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**'
  * 广告位类型标签
  */
object TagsAD extends Tags {
  /**
    * 打标签的接口
    *
    */
  override def MakeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    // 参数解析
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v > 9 => list:+("LC"+v,1)
      case v if v >0 && v <=9 => list :+("LC0"+v,1)
    }
    // 获取广告名称
    val adname = row.getAs[String]("adspacetypename")
    if(StringUtils.isNotBlank(adname)){
      list:+=("LN"+adname,1)
    }
    // 渠道标签
    val channel = row.getAs[Int]("adplatformproviderid")
    list:+=("CN"+channel,1)
    list
  }
}
