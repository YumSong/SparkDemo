package com.zhbit.spark.streaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.tools.nsc.io.Socket
import scala.util.Random

class ProductData {

  val random = new Random()

  val MaxEvents = 6

  val str = "Miguel,Eric,James,Juan,Shawn,James,Doug,Gary,Frank,Janet,Michael,James,Malinda,Mike,Elaine,Kevin,Janet,Richard,Saul,Manuela"

  val namesResource = str.split(",").toSeq

  val products = Seq(
    "iPhone Cover" -> 9.99,
    "Headphones" -> 5.49,
    "Samsung Galaxy Cover" -> 8.95,
    "iPad Cover" -> 7.49
  )

  /**
    * 创建随机的订单
    * @param n
    * @return
    */
  def generateProductEvents(n:Int) = {

    (1 to n).map{

      i =>

        val (product,price) = products(random.nextInt(products.size))

        val user = random.shuffle(namesResource).head

        (user,product,price)

    }

  }


  def sendDataBySocket(port:Int): Unit ={

    val listener = new ServerSocket(port)

    println("Listenning on port: "+port)

    while (true){

      val socket = listener.accept()

      new Thread(){

        override def run() ={

          println("Got Client connected from : "+ socket.getInetAddress)

          val out = new PrintWriter(socket.getOutputStream,true)

          while(true){

            Thread.sleep(1000)

            val num = random.nextInt(MaxEvents)

            val productEvents = generateProductEvents(num)

            productEvents.foreach{ event =>

              out.write(event.productIterator.mkString(","))

              out.write("\n")

            }

            out.flush()

            println(s"Created $num Events")

          }

          socket.close()

        }

      }.start()

    }

  }

}
