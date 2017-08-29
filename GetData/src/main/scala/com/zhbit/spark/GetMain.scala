package com.zhbit.spark

import com.zhbit.spark.spark_MLlib.{ClassificationAction, RecommendationAction}

import scala.io.Source


object GetMain extends Serializable{

  def main(args: Array[String]): Unit = {

    val ca = new ClassificationAction

    ca.checkScalerData()
  }

}
