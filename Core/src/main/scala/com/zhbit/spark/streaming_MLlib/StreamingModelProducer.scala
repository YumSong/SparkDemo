package com.zhbit.spark.streaming_MLlib


import java.io.PrintWriter
import java.net.ServerSocket

import breeze.linalg.DenseVector

import scala.util.Random

class StreamingModelProducer extends Serializable {

  val MaxEvents = 100

  val NumFeatures = 100

  val random = new Random()

  //生成服从正态分布的稠密向量的函数
  def generateRandomArray(n:Int) = Array.tabulate(n)(_ => random.nextGaussian())

  //生成一个确定的随机模型权重向量
  val w = new DenseVector(generateRandomArray(NumFeatures))

  val intercept = random.nextGaussian() * 10

  def generateNoisData(n:Int) = {

    (1 to n).map{ i =>

      val x = new DenseVector(generateRandomArray(NumFeatures))

      val y :Double = w.dot(x)

      val noisy = y + intercept

      (noisy,x)

    }

  }

  def createData(port:Int){

    val listen = new ServerSocket(port)

    println("listening on port "+ port)

    while (true){

      val socket = listen.accept()

      new Thread(){

        override def run() = {

          println("Got Client connected from "+ socket.getInetAddress)

          val out = new PrintWriter(socket.getOutputStream,true)

          while (true){

            Thread.sleep(1000)

            val num = random.nextInt(MaxEvents)

            val data = generateNoisData(num)

            data.foreach{ case (y,x) =>

                val xStr = x.data.mkString(",")

                val evenStr = s"$y\t$xStr"

                out.write(evenStr)

                out.write("\n")

            }

            out.flush()

            println(s"created $num events......")

          }

          socket.close()

        }

      }.start()

    }

  }

}
