package com.zhbit.spark

import com.zhbit.spark.spark_MLlib.{ClassModelParams, ClassificationAction, RecommendationAction}
import com.zhbit.spark.streaming.GetData


object GetMain extends Serializable{

  def main(args: Array[String]): Unit = {

    if(args.length > 0){

      val gd = new GetData

      gd.getDataByStreaming(args(0).toInt)

    }else {

      val gd = new GetData

      gd.getDataByStreaming(9999)

    }


  }

}
