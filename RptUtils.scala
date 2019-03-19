package com.Utils

/**
  * 工具类
  */
object RptUtils {
  def req(reqMode:Int,proMode:Int):List[Double]={
    if(reqMode == 1 && proMode ==1){
      // 第一个元素 原始请求数
      // 第二个元素 有效请求数
      // 第三个元素 广告请求数
      List[Double](1,0,0)
    }else if(reqMode == 1 && proMode ==2){
      List[Double](1,1,0)
    }else if(reqMode == 1 && proMode ==3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }
  def addap(iseffective:Int,isbilling:Int,
            isbid:Int,iswin:Int,adorderid:Int,winprice:Double,ad:Double):List[Double]={
    if(iseffective==1 && isbilling==1 && isbid ==1){
      if(iseffective==1 && isbilling==1 && iswin ==1 && adorderid !=0){
        List[Double](1,1,winprice/1000.0,ad/1000.0)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }
  def Counts(requestmode:Int,iseffective:Int): List[Double] ={
    if(requestmode ==2 && iseffective ==1){
      List[Double](1,0)
    }else if(requestmode ==3 && iseffective ==1){
      List[Double](0,1)
    }else{
      List[Double](0,0)
    }
  }
}
