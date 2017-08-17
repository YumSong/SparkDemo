package com.zhbit.demo.spark

import com.zhbit.demo.common.ConnetionInfo
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object Spark2Mysql {

  def main(args: Array[String]) {

    val sc = ConnetionInfo.getSc()

    val sqlContext = new SQLContext(sc)

    val prop = ConnetionInfo.getProp()

    val df:DataFrame = sqlContext.read.jdbc(ConnetionInfo.getURL(),"runcarinfo",prop)

//    df.write.mode("append").jdbc(ConnetionInfo.getNewURL(),"car",prop)

    df.registerTempTable("temp_table")

    val total_sql = "select device_id,latitude,longitude,address,target_id,gps_time from temp_table "

    val total_df: DataFrame = sqlContext.sql(total_sql)

    total_df.write.mode("append").jdbc(ConnetionInfo.getNewURL(),"newCar",prop)

    sc.stop()

  }
}
