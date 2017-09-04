package com.zhbit.spark

import com.zhbit.spark.streaming_MLlib.StreamingModelProducer


object SendMain {

  def main(args: Array[String]): Unit = {

    if (args.length > 0) {

      new StreamingModelProducer().createData(args(0).toInt)

    }else {

      new StreamingModelProducer().createData(9898)

    }
  }

}
