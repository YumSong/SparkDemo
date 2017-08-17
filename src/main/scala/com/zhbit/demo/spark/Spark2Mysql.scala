package com.zhbit.demo.spark

import java.util.Properties

import com.zhbit.demo.common.HttpUtil
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import redis.clients.jedis.JedisPool

import scala.collection.immutable.ListMap

object Spark2Mysql {

  val url = "jdbc:mysql://127.0.0.1:3306/vlpr"

  val username = "root"

  val password = "root"

  val table = "runcarinfo"

  val uri = url + "?user=" + username + "&password=" + password + "&useUnicode=true&characterEncoding=UTF-8"

  val prop = new Properties()

  prop.put("user","root")

  prop.put("password","root")

  //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误
  prop.put("driver", "com.mysql.jdbc.Driver")

  val redisHost = "192.168.0.236"

  val redisPort = 6379

  val redisTimeout = 30000

  lazy val jedisPool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  def main(args: Array[String]) {

//    val conf = new SparkConf().setAppName("Spark Sql").setMaster("spark://datanode1:7077")
//      .setJars(List("/home/song/IdeaProjects/SparkDemo/out/artifacts/SparkDemo_jar/SparkDemo.jar"))
//
//    val sc = new SparkContext(conf)
//
//    val sqlContext = new SQLContext(sc)

    val list:List[ListMap[String,String]] = HttpUtil.changeGps2Redis(jedisPool)

    println(list.toString())

//    val urlRdd = sc.parallelize(list.toSeq)

//    val schema = StructType(
//      List(
//
//        StructField("latitude",StringType,true),
//
//        StructField("longitude",StringType,true),
//
//        StructField("address",StringType,true),
//
//        StructField("device_id",StringType,true),
//
//        StructField("direction",StringType,true),
//
//        StructField("gps_time",StringType,true),
//
//        StructField("speed",StringType,true),
//
//        StructField("zhuang_tai",StringType,true)
//
//      )
//    )
//
//    //将RDD映射到rowRDD
//    val rowRDD = urlRdd.map(p => Row(
//      p.get("latitude"),
//
//      p.get("longitude"),
//
//      p.get("address"),
//
//      p.get("device_id"),
//
//      p.get("direction"),
//
//      p.get("gps_time"),
//
//      p.get("speed"),
//
//      p.get("zhuang_tai") ))
//
//    val carDataFrame = sqlContext.createDataFrame(rowRDD,schema)
//
//    carDataFrame.write.mode("append").jdbc(url,"runcarinfo",prop)
//
//    sc.stop()

  }
}
