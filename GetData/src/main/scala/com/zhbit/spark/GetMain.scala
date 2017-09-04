package com.zhbit.spark

import com.zhbit.spark.spark_MLlib.{ClassModelParams, ClassificationAction, RecommendationAction}
import com.zhbit.spark.streaming.StreamingGetData
import com.zhbit.spark.streaming_MLlib.{ StreamingMLlibMode}


object GetMain extends Serializable{

  def main(args: Array[String]): Unit = {

    if(args.length > 0){

      new StreamingMLlibMode().checkModel(args(0).toInt)

    }else {

      new StreamingMLlibMode().checkModel(9898)

    }


  }

}
