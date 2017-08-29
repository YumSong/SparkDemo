package com.zhbit.spark.spark_MLlib

import com.zhbit.spark.common.ConnetionInfo
import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithSGD, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

/**
  * spark构建分类模型
  * 现在可以对输入数据应用 MLlib 的模型。首先,需要引入必要的类并对每个模型配置一些基本的输入参数。
  * 其中,需要为逻辑回归和 SVM 设置迭代次数,为决策树设置最大树深度
  */
class ClassificationAction extends Serializable{

  ConnetionInfo.setJar("/home/song/IdeaProjects/SparkDemo/out/artifacts/GetData_jar/GetData.jar")

  val sc = ConnetionInfo.getSc("CLASSIFICATION_ACTION")

  val file = "hdfs://datanode1:9000/test/train_noheader.tsv"

  val numIterations = 10

  val maxTreeDepth = 5

  var model = null

  /**
    * 切割处理数据
    * @return
    */
  def cleanData(): RDD[Array[String]] = {

    val rawData = sc.textFile(file)

    val records = rawData.map(line => line.split("\t"))

    records

  }

  /**
    * 处理非负特征值给朴素贝叶斯分类模型训练
    * 朴素贝叶斯模型：通过给定特征（互相独立）的概率计算该类别的概率
    * @return
    */
  def getDataNotNegative(): RDD[LabeledPoint] ={

    val records = cleanData()

    val nbdata = records.map { r =>

      val trimmed = r.map(_.replaceAll("\"", ""))

      val label = trimmed(r.size - 1).toInt

      val features = trimmed.slice(4, label).map(d => if (d == "?") 0.0 else d.toDouble).map(d => if (d < 0) 0.0 else d)

      LabeledPoint(label, Vectors.dense(features))

    }

    nbdata.cache()

    nbdata

  }

  /**
    * 进一步清理数据给逻辑回归和线性支持向量机分类模型训练
    * 逻辑回归：某个数据属于正类的估计
    * SVM：w^^T *x 的估计值大于阀值，svm标为1,反之为0
    * @return
    */
  def getDataNormal(): RDD[LabeledPoint] ={

    val records = cleanData()

    val data = records.map { r =>

      val trimmed = r.map(_.replaceAll("\"", ""))

      val label = trimmed(r.size - 1).toInt

      val features = trimmed.slice(4, label).map(d => if (d == "?") 0.0 else d.toDouble)

      LabeledPoint(label, Vectors.dense(features))

    }

    data.cache()

    data

  }

  /**
    * 选择使用的模型
    * @param way
    * @return
    */
  def selectModel(way:Int):ClassificationModel = way match {

      //逻辑回归模型
    case 0 =>  LogisticRegressionWithSGD.train(getDataNormal(),numIterations)

      //SVM线性向量机模型
    case 1 =>  SVMWithSGD.train(getDataNormal(),numIterations)

      //朴素贝叶斯模型
    case 2 =>  NaiveBayes.train(getDataNotNegative())

  }

  //决策树模型
  def getDecisionTreeModel(): DecisionTreeModel ={

    DecisionTree.train(getDataNormal(),Algo.Classification,Entropy,maxTreeDepth)

  }

}
