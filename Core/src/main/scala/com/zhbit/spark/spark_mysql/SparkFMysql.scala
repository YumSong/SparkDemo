package com.zhbit.spark.spark_mysql

import com.zhbit.spark.common.ConnetionInfo
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by admin on 2017/8/16.
  */
class SparkFMysql {

  def getFMysql {

    ConnetionInfo.setJar("/home/song/IdeaProjects/SparkDemo/out/artifacts/GetData_jar/GetData.jar")

    println(ConnetionInfo.getJar())

    val sc = ConnetionInfo.getSc("getDataByMysql")

    val prop = ConnetionInfo.getProp()

    val sqlContext = new SQLContext(sc)

    val df:DataFrame = sqlContext.read.jdbc(ConnetionInfo.getNewURL(),"newCar",prop)
    while (true){

      println(df.count())

      Thread.sleep(3000)
    }

  }



}
