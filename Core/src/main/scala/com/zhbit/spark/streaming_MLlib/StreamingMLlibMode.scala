package com.zhbit.spark.streaming_MLlib

import breeze.linalg.DenseVector
import com.zhbit.spark.common.ConnetionInfo
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, StreamingLinearRegressionWithSGD}
import org.apache.spark.streaming.dstream.DStream

class StreamingMLlibMode {

  ConnetionInfo.setMaster("local[2]")

  @transient
  val ssc = ConnetionInfo.getSsc("GET_DATA",10)

  def calDataByStreaming(port:Int): Unit ={

    val model = getModel(0.01,1)

    val labeledStream = getStream(port)

    model.trainOn(labeledStream)

    model.predictOn(labeledStream.map(_.features)).print()

    ssc.start()

    ssc.awaitTermination()
  }

  def checkModel(port:Int): Unit ={

    println("Enter into checkModel")

    val model1 = getModel(0.01,1)

    val model2 = getModel(1.0,1)

    val labelStream = getStream(port)

    model1.trainOn(labelStream)

    model2.trainOn(labelStream)

    val predsAndTrue = labelStream.transform{ rdd =>

      val lastest1 = model1.latestModel()

      val lastest2 = model2.latestModel()

      rdd.map{ point =>

        val pred1 = lastest1.predict(point.features)

        val pred2 = lastest2.predict(point.features)

        ((pred1 - point.label), (pred2 - point.label))
      }

    }

    predsAndTrue.foreachRDD{ (rdd,time) =>

      val mse1 = rdd.map{ case (err1,err2) =>

        err1*err1

      }.mean()

      val mse2 = rdd.map{ case (err1,err2) =>

        err2*err2

      }.mean()

      val rmse1 = math.sqrt(mse1)

      val rmse2 = math.sqrt(mse2)

      println(

        s"""
           |-----------------------------------
           |Time : $time
           |-----------------------------------
         """.stripMargin
      )

      println(s" MSE current batch : MODEL1 is  $mse1")

      println(s" MSE current batch : MODEL2 is  $mse2")

      println("----------------------------------------")

      println(s" RMSE current batch : MODEL1 is  $rmse1")

      println(s" RMSE current batch : MODEL2 is  $rmse2")

      println("----------------------------------------")

    }

    ssc.start()

    ssc.awaitTermination()
  }

  def getStream(port:Int): DStream[LabeledPoint] ={

    val stream = ssc.socketTextStream("192.168.0.236",port)

    val labeledStream:DStream[LabeledPoint] = stream.map{ event =>

      val split = event.split("\t")

      val y = split(0).toDouble

      val features = split(1).split(",").map(_.toDouble)

      LabeledPoint(label = y,features = Vectors.dense(features))
    }

    labeledStream
  }

  def getModel(StepSize:Double,Iteration:Int): StreamingLinearRegressionWithSGD ={

    val NumFeatures = 100

    val zeroVector = DenseVector.zeros[Double](NumFeatures)

    val model = new StreamingLinearRegressionWithSGD()
      .setStepSize(StepSize)
      .setNumIterations(Iteration)
      .setInitialWeights(Vectors.dense(zeroVector.data))

    model

  }

}
