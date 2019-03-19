package com.Tag

/**
  * 公共接口
  */
trait Tags {
  /**
    * 打标签的接口
    *
    */
  def MakeTags(args:Any*):List[(String,Int)]
}
