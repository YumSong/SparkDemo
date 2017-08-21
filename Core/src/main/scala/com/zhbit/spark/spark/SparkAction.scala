package com.zhbit.spark.spark

import java.util.Date

import com.zhbit.spark.common.ConnetionInfo
import org.apache.spark.mllib.recommendation.{ALS, Rating}

class SparkAction {

  def dealData(): Unit ={

    println("enter test")

    ConnetionInfo.setJar("/home/song/IdeaProjects/SparkDemo/out/artifacts/GetData_jar/GetData.jar")

    ConnetionInfo.setMaster("local[2]")

    val sc = ConnetionInfo.getSc("Test")

    Array(1.9, 2.9, 3.4, 3.5)

    val args = Array ("John,iphone,9.99","John,Samsung,8.95","Jack,iphone,9.99","Jill,Samsung,8.95","Bob,iPad,5.49","Jack,iphone,9.99")



//    val data = sc.parallelize(args)
//      .map(line => line.split(","))
//      .map(re => (re(0),re(1),re(2)))

    val file = "file:///home/song/data.csv"

    val data = sc.textFile(file)
      .map(line => line.split(","))
      .map(re => (re(0),re(1),re(2)))

    //求购买的次数
    val num_Buy = data.count()

    //多个不同的客户买过商品
    val uniqueUsers = data.map{ case(user,product,price)=>user }.distinct().count()

    //求总收入
    val totalRevenue = data.map{ case(user,product,price)=>price.toDouble }.sum()

    //畅销书
    val productsByPopularity = data
      .map{ case(user,product,price) => (product,1) }
      .reduceByKey(_+_)
      .collect()
      //-_._2的含义  "-"表示递减, "._2"表示比较的是集合里面每条记录的第二个元素的大小
      .sortBy(line => -line._2)

    //取出最高数量的第一条
    val mostPopular = productsByPopularity(0)

    while (true){

      println("购买的次数： "+num_Buy)

      println("顾客数： "+uniqueUsers)

      println("总收入： "+totalRevenue)

      println("畅销书： "+mostPopular)

      Thread.sleep(3000)

    }

  }


  def getUserData(): Unit ={

    println("Test  "+new Date())

    ConnetionInfo.setJar("/home/song/IdeaProjects/SparkDemo/out/artifacts/GetData_jar/GetData.jar")

//    ConnetionInfo.setMaster("local[2]")

    val sc = ConnetionInfo.getSc("getUserData")

//    val line = sc.textFile("file:///home/song/ml-100k/u.user").map(line => line.split(","))

    val line = sc.textFile("hdfs://datanode1:9000/test/u.user").map(line => line.split(","))

    val num_user = line.map(s => s(0)).count()

    val num_genders = line.map(s => s(2)).distinct().count()

    val num_occupations = line.map(s => s(3)).distinct().count()

    val num_zipcodes = line.map(s => s(4)).distinct().count()

    println("User: "+num_user)

    println("genders: "+num_genders)

    println("occupations: "+num_occupations)

    println("ZIP codes: "+num_zipcodes)

  }


  def getFilmData(): Unit ={

    println("getFilmData  "+new Date())

    ConnetionInfo.setJar("/home/song/IdeaProjects/SparkDemo/out/artifacts/GetData_jar/GetData.jar")

    //    ConnetionInfo.setMaster("local[2]")

    val sc = ConnetionInfo.getSc("getFilmData")

    //    val line = sc.textFile("file:///home/song/ml-100k/u.user").map(line => line.split(","))

    val rawData = sc.textFile("hdfs://datanode1:9000/test/u.data")

    val rawRatings = rawData.map(line =>line.split("\t").take(3))

    val ratings = rawRatings.map { case Array(user, movie, rating) =>
      Rating(user.toInt, movie.toInt, rating.toDouble) }

    val model = ALS.train(ratings, 50, 10, 0.01)

    println(model.userFeatures.count())

    println(model.productFeatures.count())

  }

}
