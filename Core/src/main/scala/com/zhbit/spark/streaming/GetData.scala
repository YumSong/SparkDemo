package com.zhbit.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.zhbit.spark.common.ConnetionInfo
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

class GetData extends Serializable {

  ConnetionInfo.setMaster("local[2]")

  @transient
  val ssc = ConnetionInfo.getSsc("GET_DATA",10)



  /**
    * 监听端口获取数据
    * @param port
    */
  def getDataByStreaming(port:Int): Unit ={

    ssc.checkpoint("/tmp/sparkstreaming")

    val stream = ssc.socketTextStream("192.168.0.236",port)

//    showEvents(stream)

//    updateEvent(stream)

    updateEventByWindow(stream)

    ssc.start()

    ssc.awaitTermination()
  }


  /**
    * 展示events里面的数据
    * @param stream
    */
  def showEvents(stream:DStream[String]): Unit ={

    val events = stream.map{ record =>

      val event = record.split(",")

      (event(0), event(1), event(2))

    }

    events.foreachRDD { (rdd,time) =>

      if( rdd.count() > 0 ){

        val numPurchases = rdd.count()

        val uniqueUsers = rdd.map{

          case(user,_,_) => user

        }.distinct().count()

        val totalRevenue = rdd.map{

          case(_,_,price) => price.toDouble

        }.sum()

        val produceByPopularity = rdd.map{

          case(user,product,price) => (product,1)

        }.reduceByKey( _ + _ ).collect().sortBy(-_._2)

        var mostPopular = produceByPopularity(0)

        val formatter = new SimpleDateFormat

        val dateStr = formatter.format(new Date(time.milliseconds))

        println(s"== Batch start time: $dateStr ==")

        println("Total purchases: " + numPurchases)

        println("Unique users: " + uniqueUsers)

        println("Total revenue: " + totalRevenue)

        println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))

      }

    }

  }


  /**
    * 通过updateState的方式进行有状态的计算
    * @param stream
    */
  def updateEvent(stream: DStream[String]) = {

    val events = stream.map{ records =>

      val event = records.split(",")

      (event(0),event(1),event(2).toDouble)

    }

    val users = events.map{ case (user,product,price) =>

      (user,(product,price))

    }

    val revenuePerUser = users.updateStateByKey(updateState)

    revenuePerUser.print()

  }


  /**
    * 通过滑动窗口的方式进行DStream的有状态的流计算
    * @param stream
    */
  def updateEventByWindow(stream:DStream[String]) ={

    val events = stream.map{ records =>

      val event = records.split(",")

      (event(0),event(1),event(2).toDouble)

    }

    val user = events.map{ case (user,product,price) =>

      (user,1)

    }

    val price = events.map{ case (user,product,price) =>

      (user,price)

    }

    val searchUser = user.reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(60), Seconds(10))

    val searchPrice = price.reduceByKeyAndWindow((v1: Double, v2: Double) => v1 + v2, Seconds(60), Seconds(10))

    val totalData = searchUser.join(searchPrice)

    totalData.print()

  }


  /**
    * 两次状态的叠加
    * @param prices  物品的名称和价格
    * @param currentTotal 物品数量和价格
    * @return
    */
  def updateState(prices: Seq[(String, Double)], currentTotal: Option[(Int, Double)]) ={

    //计算第一批物品的总价
    val currentRevenue = prices.map(_._2).sum

    //计算第一批物品的数量
    val currentNumPurchases = prices.size

    //判断是否有新的RDD进入
    val state = currentTotal.getOrElse(0,0.0)

    //当前物品的数量和下一个批次的物品数量的相加,价格的相加
    Some((currentNumPurchases + state._1 , currentRevenue + state._2))

  }


}
