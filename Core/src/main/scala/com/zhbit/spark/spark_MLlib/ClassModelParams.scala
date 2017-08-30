package com.zhbit.spark.spark_MLlib

import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithSGD, NaiveBayes}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.optimization.{SimpleUpdater, SquaredL2Updater, Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.{Entropy, Impurity}
import org.apache.spark.rdd.RDD

/**
  * 模型参数调优
  */
class ClassModelParams {

  /** ***************************线性模型 ****************************************/
  /**
    * 带参数训练
    *
    * @param input 输入的数据集
    * @param regParam
    * @param numIterations
    * @param updater
    * @param stepSize
    * @return
    */
  def trainWithParamsByLinear(input: RDD[LabeledPoint], regParam: Double, numIterations: Int, updater: Updater, stepSize: Double) = {

    val lr = new LogisticRegressionWithSGD()

    lr.optimizer
      .setRegParam(regParam)
      .setUpdater(updater)
      .setNumIterations(numIterations)
      .setStepSize(stepSize)

    lr.run(input)

  }


  /**
    * 计算ROC的AUC
    *
    * @param label
    * @param data
    * @param model
    * @return
    */
  def createMetrics(label: String, data: RDD[LabeledPoint], model: ClassificationModel) = {

    val scoreAndLabels = data.map { point =>

      (model.predict(point.features), point.label)

    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)

    (label, metrics.areaUnderROC())

  }


  /**
    * 获取标准化数据，并缓存
    *
    * @return
    */
  def getScalerDateCat() = {

    val action = new ClassificationAction

    val data = action.addClassData()

    val vectors = data.map(lp => lp.features)

    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)

    val scaledDataCat = data.map(lp => LabeledPoint(lp.label, scaler.transform(lp.features)))

    scaledDataCat.cache()

    scaledDataCat
  }


  /**
    * 1、迭代计算
    * 大多数机器学习的方法需要迭代训练,并且经过一定次数的迭代之后收敛到某个解(即最小化损失函数时的最优权重向量)。
    * SGD 收敛到合适的解需要迭代的次数相对较少,但是要进一步提升性能则需要更多次迭代。
    */
  def iterationCal(): Unit = {

    val scaledDataCat = getScalerDateCat()

    val iterResults = Seq(1, 5, 10, 50).map { param =>

      val model = trainWithParamsByLinear(scaledDataCat, 0.0, param, new SimpleUpdater, 1.0)

      createMetrics(s"$param iterations", scaledDataCat, model)

    }

    iterResults.foreach {

      case (param, auc) =>

        println(f"$param ,AUC = ${auc * 100}%2.2f%%")

    }

  }


  /**
    * 2、步长计算
    * 在 SGD 中,在训练每个样本并更新模型的权重向量时,
    * 步长用来控制算法在最陡的梯度方向上应该前进多远。
    * 较大的步长收敛较快,但是步长太大可能导致收敛到局部最优解。
    */
  def stepCal(): Unit = {

    val scaledDataCat = getScalerDateCat()

    val iterResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>

      val model = trainWithParamsByLinear(scaledDataCat, 0.0, numIterations = 5, new SimpleUpdater, param)

      createMetrics(s"$param iterations", scaledDataCat, model)

    }

    iterResults.foreach {

      case (param, auc) =>

        println(f"$param ,AUC = ${auc * 100}%2.2f%%")

    }
  }

  /**
    * 3、正则化计算
    * SimpleUpdater       默认正则化
    * SquaredL2Updater    基于权重向量L2的正则化，SVM的默认值
    * L1Updater           这个正则项基于权重向量的 L1 正则化,会导致得到一个稀疏的权重向量(不重要的权重的值接近 0 )
    */
  def updaterCal(): Unit = {

    val scaledDataCat = getScalerDateCat()

    val iterResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>

      val model = trainWithParamsByLinear(scaledDataCat, 0.0, numIterations = 5, new SquaredL2Updater, param)

      createMetrics(s"$param iterations", scaledDataCat, model)

    }

    iterResults.foreach {

      case (param, auc) =>

        println(f"$param ,AUC = ${auc * 100}%2.2f%%")

    }
  }


  /** ***************************决策树 ****************************************/

  def getNormalData() = {

    val action = new ClassificationAction

    action.getDataNormal()

  }

  def trainDTwithParams(input: RDD[LabeledPoint], maxDepth: Int, impurity: Impurity) = {

    DecisionTree.train(input, Algo.Classification, impurity, maxDepth)

  }

  def treeDepthCal(): Unit = {

    val data = getNormalData()

    val dtResultsEntropy = Seq(1, 2, 3, 4, 5, 10, 20).map { param =>

      val model = trainDTwithParams(data, param, Entropy)

      val scoreAndLabels = data.map { point =>

        val score = model.predict(point.features)

        (if (score > 0.5) 1.0 else 0.0, point.label)

      }

      val metrics = new BinaryClassificationMetrics(scoreAndLabels)

      (s"$param tree depth", metrics.areaUnderROC())

    }

    dtResultsEntropy.foreach { case (param, auc) =>
      println(f"$param ,AUC = ${auc * 100}%2.2f%%")

    }

  }


  /** ***************************决策树 ****************************************/

  def trainNBWithParam(input:RDD[LabeledPoint],lamba:Double) ={

    val model = new NaiveBayes

    model.setLambda(lamba)

    model.run(input)

  }

  def getDataNB() ={

    val action = new ClassificationAction

    action.getDataNotNegative()

  }

  def nbCal(): Unit ={

    val data = getDataNB()

    val iterResults = Seq(0.001, 0.01, 0.1, 1.0, 10.0).map { param =>

      val model = trainNBWithParam(data, param)

      val scoreAndLabels = data.map{ point =>

        (model.predict(point.features),point.label)

      }

      val metrics = new BinaryClassificationMetrics(scoreAndLabels)

      (s"$param lambda", metrics.areaUnderROC)
    }

    iterResults.foreach {

      case (param, auc) =>

        println(f"$param ,AUC = ${auc * 100}%2.2f%%")

    }

  }
}




