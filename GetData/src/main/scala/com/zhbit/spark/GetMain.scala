package com.zhbit.spark

import com.zhbit.spark.spark.SparkAction

import scala.io.Source


object GetMain extends Serializable{

  def main(args: Array[String]): Unit = {

    val sa = new SparkAction

    sa.getFilmData()

  }

}
