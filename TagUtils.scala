package com.Utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 标签工具类
  */
object TagUtils {
  // 获取用户唯一的ID
  def getAnyOneUserId (row:Row):String ={
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "IM:"+v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => "MC:"+v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => "ID:"+v.getAs[String]("idfa")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => "OD:"+v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) => "AOD:"+v.getAs[String]("androidid")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => "MD5IM:"+v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => "MD5MC:"+v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => "MD5ID:"+v.getAs[String]("idfamd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => "MD5OD:"+v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => "MD5AOD:"+v.getAs[String]("androididmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => "SHIM:"+v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) => "SHMC:"+v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => "SHID:"+v.getAs[String]("idfasha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) => "SHOD:"+v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => "SHAOD:"+v.getAs[String]("androididsha1")
    }
  }
  // 过滤数据
  val OneUserId =
    """
      |imei !='' or mac !='' or idfa!='' or openudid !='' or androidid !='' or
      |imeimd5 !='' or macmd5 !='' or idfamd5 !='' or
      |openudidmd5 !='' or androididmd5 !='' or imeisha1 !='' or macsha1 !='' or
      |idfasha1 !='' or openudidsha1 !='' or androididsha1!=''
    """.stripMargin
  // 获取用户全部的ID
  def getUserIdAll(row:Row)={
    var list = List[String]()
    if(StringUtils.isNoneEmpty(row.getAs("imei"))) list:+="IM"+row.getAs[String]("imei")
    if(StringUtils.isNoneEmpty(row.getAs("mac"))) list:+="MC"+row.getAs[String]("mac")
    if(StringUtils.isNoneEmpty(row.getAs("idfa"))) list:+="ID"+row.getAs[String]("idfa")
    if(StringUtils.isNoneEmpty(row.getAs("openudid"))) list:+="OD"+row.getAs[String]("openudid")
    if(StringUtils.isNoneEmpty(row.getAs("androidid"))) list:+="AOD"+row.getAs[String]("androidid")
    if(StringUtils.isNoneEmpty(row.getAs("imeimd5"))) list:+="MD5IM"+row.getAs[String]("imeimd5")
    if(StringUtils.isNoneEmpty(row.getAs("macmd5"))) list:+="MD5MC"+row.getAs[String]("macmd5")
    if(StringUtils.isNoneEmpty(row.getAs("idfamd5"))) list:+="MD5ID"+row.getAs[String]("idfamd5")
    if(StringUtils.isNoneEmpty(row.getAs("openudidmd5"))) list:+="MD5OD"+row.getAs[String]("openudidmd5")
    if(StringUtils.isNoneEmpty(row.getAs("androididmd5"))) list:+="MD5AOD"+row.getAs[String]("androididmd5")
    if(StringUtils.isNoneEmpty(row.getAs("macsha1"))) list:+="SHMC"+row.getAs[String]("macsha1")
    if(StringUtils.isNoneEmpty(row.getAs("idfasha1"))) list:+="SHID"+row.getAs[String]("idfasha1")
    if(StringUtils.isNoneEmpty(row.getAs("openudidsha1"))) list:+="SHOD"+row.getAs[String]("openudidsha1")
    if(StringUtils.isNoneEmpty(row.getAs("androididsha1"))) list:+="SHAOD"+row.getAs[String]("androididsha1")
    if(StringUtils.isNoneEmpty(row.getAs("imeisha1"))) list:+="SHIM"+row.getAs[String]("imeisha1")
    list
  }
}
