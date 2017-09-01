package com.zhbit.spark

import com.zhbit.spark.streaming.{GetData, ProductData}


object SendMain {

  def main(args: Array[String]): Unit = {

    if (args.length > 0) {

      val pd = new ProductData

      pd.sendDataBySocket(args(0).toInt)

    }else {

      val pd = new ProductData

      pd.sendDataBySocket(9999)

    }
  }

}
