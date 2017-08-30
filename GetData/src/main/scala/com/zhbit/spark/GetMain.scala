package com.zhbit.spark

import com.zhbit.spark.spark_MLlib.{ClassModelParams, ClassificationAction, RecommendationAction}


object GetMain extends Serializable{

  def main(args: Array[String]): Unit = {

    val cp = new ClassModelParams

    cp.nbCal()

  }

}
