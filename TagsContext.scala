package com.Tag

import com.Utils.{App2Jedis, TagUtils}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 上下文标签
  */
object TagsContext {
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
    df2.mapPartitions(row=>{
      val list = collection.mutable.ListBuffer[(String,List[(String, Int)])]()
      val jedis = App2Jedis.getConnection()
      row.foreach(row=>{
        // 根据每一条数据  打对应的标签 （7种）
        // 获取用户的ID
        val userId = TagUtils.getAnyOneUserId(row)
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
        // 商圈标签
        val business = TagsBusiness.MakeTags(row,jedis)
        // 将所有的标签进行合并
        list +=((userId,adTag++appTag++deviceTag++KwTag++LocationTag++business))
      })
      jedis.close()
      list.iterator
    }).reduceByKey(
      // ((ln 爱奇艺，1),(ky 武侠电影，1),(D00010001，1))
      (list1,list2)=>(list1:::list2)
        .groupBy(_._1) // (ln 爱奇艺，List(1,1,1,1,1,1,1,1))
        .mapValues(_.foldLeft[Int](0)(_ + _._2))
        .toList
    )
      // 将数据存入Hbase
      .map{
      case (userid,userTags) =>{
        val put = new Put(Bytes.toBytes(userid))
        val tags = userTags.map(t=>t._1+":"+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(s"$day"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)

  }
}
