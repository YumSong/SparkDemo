package com.zhbit.spark.spark

import com.zhbit.spark.common.ConnetionInfo
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

class SparkAction extends Serializable {

  ConnetionInfo.setJar("/home/song/IdeaProjects/SparkDemo/out/artifacts/GetData_jar/GetData.jar")

  ConnetionInfo.setAppName("SparkAction")

//  ConnetionInfo.setMaster("local[2]")

  @transient
  var sc = ConnetionInfo.getSc()


  /**
    * 处理rdd数据的简单demo
    */
  def dealData(): Unit ={

    println("enter test")

    ConnetionInfo.setJar("/home/song/IdeaProjects/SparkDemo/out/artifacts/GetData_jar/GetData.jar")

    ConnetionInfo.setMaster("local[2]")

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


  /**
    * 从hdfs文件系统中获取用户信息
    */
  def getUserData(): Unit ={

    val line = sc.textFile("hdfs://datanode1:9000/test/u.user").map(line => line.split(","))

//    val line = sc.textFile("file:///home/song/ml-100k/u.user").map(line => line.split(","))

    val num_user = line.map(s => s(0)).count()

    val num_genders = line.map(s => s(2)).distinct().count()

    val num_occupations = line.map(s => s(3)).distinct().count()

    val num_zipcodes = line.map(s => s(4)).distinct().count()

    println("User: "+num_user)

    println("genders: "+num_genders)

    println("occupations: "+num_occupations)

    println("ZIP codes: "+num_zipcodes)

  }


  /**
    * 获取电影信息的一些方法，例如计算评级等
    */
  def getFilmData(): Unit ={

    println("enter into getFilmData")

    //提取出电影的title数组
    val title = getTitle()

    //ALS 训练数据集，提取出模型
    val model = ALS.train(getRatings(), 50, 10, 0.01)

    /**
      * 查看推荐的结果
      */
    val userId = 789

    val k = 10

    //预测前十名
    val topKRecs = model.recommendProducts(userId,k)

    val movieForUser = getRatings().keyBy(_.user).lookup(789)

    //展示推荐数据
//    showData(movieForUser,topKRecs,model,title)

    //计算均方差
//    calBySquart(movieForUser,model,ratings)

    println()

    //计算K值平均率
    calByMAPK(movieForUser,model,topKRecs)

  }


  /**
    * 获取文件的评价数据集合
    * @return
    */
  def getRatings(): RDD[Rating] ={

    val rawData = sc.textFile("hdfs://datanode1:9000/test/u.data")

    //    val rawData = sc.textFile("file:///home/song/ml-100k/u.data")

    val rawRatings = rawData.map(line =>line.split("\t").take(3))

    val ratings = rawRatings.map { case Array(user, movie, rating) =>
      Rating(user.toInt, movie.toInt, rating.toDouble) }

    ratings

  }


  /**
    * 获取文件的标题集合
    * @return
    */
  def getTitle(): collection.Map[Int, String] ={

    val movies = sc.textFile("hdfs://datanode1:9000/test/u.item")

    //    val movies = sc.textFile("file:///home/song/ml-100k/u.item")

    //提取出电影的title数组
    val title = movies.map(line => line.split("\\|").take(2)).map(array => (array(0).toInt,array(1))).collectAsMap()

    title

  }


  /**
    * 展示推荐的数据等
    * @param movieForUser
    * @param topKRecs
    * @param model
    * @param title
    */
  def showData(movieForUser: Seq[Rating], topKRecs: Array[Rating], model: MatrixFactorizationModel, title: collection.Map[Int, String]) = {

    println(title(123))

    println(topKRecs.mkString("\n"))

    println("num is "+model.predict(789,123))

    println("这个用户看的电影数是： "+movieForUser.size)

    println("数据集排序：")
    movieForUser.sortBy(-_.rating).take(10).map(rating => (title(rating.product),rating.rating)).foreach(println)

    println("智能推荐：")
    topKRecs.map(rating => (title(rating.product),rating.rating)).foreach(println)
  }


  /**
    * 计算MAPK
    * @param movieForUser
    * @param model
    * @param topKRecs
    */
  def calByMAPK(movieForUser: Seq[Rating], model: MatrixFactorizationModel, topKRecs: Array[Rating]) = {

    val actualMovices = movieForUser.map(_.product)

    val predictMovies = topKRecs.map(_.product)

    println(actualMovices)

    println(predictMovies)

    val apk10 = avgPrecisionK(actualMovices, predictMovies, 10)

    println("APK is " + apk10)

    //计算MAPK
   val itemFactors = model.productFeatures.map{
      case (id,factor) => factor
    }.collect()

    val itemMarix = new DoubleMatrix(itemFactors)

    println(itemMarix)

    println(itemMarix.rows,itemMarix.columns)

    val imBroadcast = sc.broadcast(itemMarix)

    val allRecs = model.userFeatures.map{

      case (userId,array) =>

        val userVector = new DoubleMatrix(array)

        val scores = imBroadcast.value.mmul(userVector)

        val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)

        val recommededIds = sortedWithId.map(_._2+1).toSeq

        (userId,recommededIds)
    }

    val userMovice = getRatings().map{

      case Rating(user,product,rating) => (user,product)

    }.groupBy(_._1)


    val MAPK = allRecs.join(userMovice).map{

      case (userId,(predicted,actualWithIds)) =>

        val K = 10

        val actual = actualWithIds.map(_._2).toSeq

        avgPrecisionK(actual,predicted,K)

    }.reduce(_ + _) /allRecs.count()

    println("MAPK is " + MAPK)
  }


  /**
    * 计算均方差
    * @param movieForUser
    * @param model
    * @param ratings
    */
  def calBySquart(movieForUser:Seq[Rating],model:MatrixFactorizationModel,ratings: RDD[Rating]): Unit ={

    //第一条数据的的均方差
    val actualRating = movieForUser.take(1)(0)

    println(actualRating)

    val predictedRating = model.predict(789,actualRating.product)

    println(predictedRating)

    val squaredError = math.pow(predictedRating-actualRating.rating,2.0)

    println(squaredError)

    //将rating里面的用户和产品作为主键
    val userProduct = ratings.map{ case Rating(user,product,rating) => (user,product)}

    //模型预测（用户，产品）这个rdd 的评级RDD
    val prediction = model.predict(userProduct).map{
      case Rating(user,product,rating) =>((user,product),rating)
    }

    //模型预测的评级RDD jion进数据集里面的评级rdd
    val ratingAndPredictions = ratings.map{
      case Rating(user,product,rating) =>((user,product),rating)
    }.join(prediction)

    //计算整个数据集的方差
    //reduce里面的二元指“(user,produce)”和“math.pow((actual-predicted),2)”这个两个元素
    val Mse = ratingAndPredictions.map{
      //计算每个元素的误差
      case ((user,produce),(actual,predicted)) => math.pow((actual-predicted),2)
    }.reduce(_ + _) / ratingAndPredictions.count()

    println("MEAN SQUARED ERROR = "+Mse)

    //计算出均方差
    val RMSE = math.sqrt(Mse)

    println("Root Mean Squared Error = " + RMSE)

  }


  /**
    * 计算APK
    * @param actual
    * @param predicted
    * @param k
    * @return
    */
  def avgPrecisionK(actual:Seq[Int],predicted:Seq[Int],k:Int) :Double ={

    val predk = predicted.take(k)

    var score = 0.0

    var numHits = 0.0

    for((p,i) <- predk.zipWithIndex){

      if(actual.contains(p)){

        numHits += 1.0

        score += numHits / (i.toDouble +1.0)

      }

    }

    if(actual.isEmpty){

      1.0

    } else {

      score/math.min(actual.size,k).toDouble

    }
  }

}
