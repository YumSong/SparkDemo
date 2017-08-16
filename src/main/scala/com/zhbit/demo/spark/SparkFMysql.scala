package com.zhbit.demo.spark


import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2017/8/16.
  */
object SparkFMysql {

  val url = "jdbc:mysql://192.168.0.223:3306/vlpr"

  val username = "root"

  val password = "root"

  val table = "runcarinfo"

  val uri = url + "?user=" + username + "&password=" + password + "&useUnicode=true&characterEncoding=UTF-8"

  val prop = new Properties()
  //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误
  prop.put("driver", "com.mysql.jdbc.Driver")

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi Test").setMaster("spark://datanode1:7077")
      .setJars(List("F:\\java_project\\SparkDemo\\out\\artifacts\\SparkDemo_jar\\SparkDemo.jar"));

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

//    val reader = sqlContext.read.format("jdbc")
//
//    reader.option("url", url)
//
//    reader.option("dbtable", table)
//
//    reader.option("driver", "com.mysql.jdbc.Driver")
//
//    reader.option("user", "root")
//
//    reader.option("password", "root")

    val df:DataFrame = sqlContext.read.jdbc(uri,"runcarinfo",prop)

    df.select("*").collect().foreach(println)

  }


}
