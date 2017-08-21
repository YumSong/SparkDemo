package com.zhbit.spark.spark_mysql

import com.zhbit.spark.common.ConnetionInfo
import org.apache.spark.sql.{DataFrame, SQLContext}

class Spark2Mysql {

  def send2Mysql(args: Array[String]) {

//    /home/song/IdeaProjects/SparkDemo/out/artifacts/SendData_jar
    ConnetionInfo.setJar("/home/song/IdeaProjects/SparkDemo/out/artifacts/SendData_jar/SendData.jar")

    val sc = ConnetionInfo.getSc("sendDataToMysql")

    val sqlContext = new SQLContext(sc)

    val prop = ConnetionInfo.getProp()

    val df:DataFrame = sqlContext.read.jdbc(ConnetionInfo.getURL(),"runcarinfo",prop)

//    df.write.mode("append").jdbc(ConnetionInfo.getNewURL(),"car",prop)

    df.registerTempTable("temp_table")

    val total_sql = "select device_id,latitude,longitude,address,target_id,gps_time from temp_table "

    val total_df: DataFrame = sqlContext.sql(total_sql)
    while (true){

      println("enter")

      total_df.write.mode("append").jdbc(ConnetionInfo.getNewURL(),"newCar",prop)

      Thread.sleep(10000)

    }

    sc.stop()

  }
}
