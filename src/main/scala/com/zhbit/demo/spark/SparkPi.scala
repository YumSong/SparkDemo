package com.zhbit.demo.spark
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.random
object SparkPi {
  def main(args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("Spark Pi Test").setMaster("spark://datanode1:7077")
      .setJars(List("F:\\java_project\\SparkDemo\\out\\artifacts\\SparkDemo_jar\\SparkDemo.jar"));
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
