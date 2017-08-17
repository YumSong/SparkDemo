package com.zhbit.demo.spark


import java.util.Properties

import com.zhbit.demo.common.ConnetionInfo
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by admin on 2017/8/16.
  */
object SparkFMysql {

  def main(args: Array[String]) {

    val sc = ConnetionInfo.getSc()

    val prop = ConnetionInfo.getProp()

    val sqlContext = new SQLContext(sc)

    val df:DataFrame = sqlContext.read.jdbc(ConnetionInfo.getNewURL(),"newCar",prop)

    df.select("*").show()
  }



}
