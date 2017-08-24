package com.zhbit.spark

import com.zhbit.spark.spark.RecommendationAction

import scala.io.Source


object GetMain extends Serializable{

  def main(args: Array[String]): Unit = {

    val sa = new RecommendationAction

    sa.getFilmData()

  }

}
