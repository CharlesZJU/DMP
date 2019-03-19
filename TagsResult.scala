package com.Tag

import com.Utils.TagUtils
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 最终版标签
  */
object TagsResult {
  def main(args: Array[String]): Unit = {
    // 模拟企业编程 首先判断目录是否为空
    if(args.length != 5){
      println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个数组存储输入输出目录
    val Array(inputPath,outputPath,dirPath,stopWord,day) = args
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
      // 默认是java序列化方式，我们需要改成scala序列化方式，这样可以提高效率
      // 因为scala的序列化方式比java的序列化方式体积小，速度快，要比java快10倍
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    // 加载配置文件
    val load = ConfigFactory.load()
    // 获取Hbase表名
    val hbaseTableName = load.getString("hbase.table.name")
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))
    // 得到HBASE的connection连接
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      println("表名可以使用！！！")
      // 创建表
      val tableName = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      // 创建列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      // 将列簇加入表中
      tableName.addFamily(columnDescriptor)
      hbadmin.createTable(tableName)
      // 关连接
      hbadmin.close()
      hbconn.close()
    }
    // 创建job
    val jobConf = new JobConf(configuration)
    // 指定Key的输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // 指定到输出哪张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    // 读取字典文件
    val dirMap = sc.textFile(dirPath).map(_.split("\t",-1))
      .filter(_.length >= 5).map(arr=>(arr(4),arr(1))).collect.toMap
    // 广播出去
    val cast = sc.broadcast(dirMap)
    // 停用词库
    val stopwords = sc.textFile(stopWord).map((_,0)).collect.toMap
    // 广播出去
    val stopWordCast = sc.broadcast(stopwords)
    // 读取数据源
    val df = sqlContext.read.parquet(inputPath)
    // 过滤数据
    val df2 = df.filter(TagUtils.OneUserId)
    // 处理用户ID
    val baseRDD = df2.map(row=>{
      // 获取当前行上的所有的用户非空ID
      val userAllId = TagUtils.getUserIdAll(row)
      (userAllId,row)
    })
    // 构建点的集合
    val verites = baseRDD.flatMap(t=>{
      val row = t._2
      // 把标签处理一下
      // 广告位类型 和 渠道标签
      val adTag: List[(String, Int)] = TagsAD.MakeTags(row)
      // APP标签
      val appTag = TagsAPP.MakeTags(row,cast)
      // 设备标签
      val deviceTag = TagsDevice.MakeTags(row)
      // 关键字标签
      val KwTag = TagsWord.MakeTags(row,stopWordCast)
      // 地域标签
      val LocationTag = TagsLocation.MakeTags(row)
      // 先把标签加到一起
      val Tags = adTag ++ appTag++ deviceTag++ KwTag ++ LocationTag
      // 只有一个人携带标签数据就好，其他ID不能带，如果同一个行上的多个ID都携带了标签，
      // 标签里面的值不准确了，到最后聚合时候，数据翻倍，这样肯定不行
      val VD = t._1.map((_,0)) ++ Tags
      // 我们要保证数据统一性，为了取顶点ID，所以要把所有的ID转换成HashCore值
      t._1.map(uid=>{
        if(t._1.head.equals(uid)){
          (uid.hashCode.toLong,VD)
        }else {
          (uid.hashCode.toLong,List.empty)
        }
      })
    })
    //verites.take(50).foreach(println)
    // 构造边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(t => {
      // A B C  a->b  a->c
      t._1.map(uid => Edge(t._1.head.hashCode, uid.hashCode.toLong, 0))
    })
    //edges.take(20).foreach(println)
    // 调用图计算  找出其中的顶点，进行连接
    val graph = Graph(verites,edges)
    // 取顶点
    val cc = graph.connectedComponents().vertices
    // 聚合数据
    cc.join(verites).map{
      case (uid,(comId,tagAndUserId)) =>(comId,tagAndUserId)
    }.reduceByKey{
      case (list1,list2) =>(list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }.take(20).foreach(println)

    sc.stop()
  }
}
