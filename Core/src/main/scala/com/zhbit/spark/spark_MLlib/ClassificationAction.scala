package com.zhbit.spark.spark_MLlib

import com.zhbit.spark.common.ConnetionInfo
import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithSGD, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.feature.StandardScaler

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


  /*************************************数据前期处理*************************************/
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

      val features = trimmed.slice(4, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble).map(d => if (d < 0) 0.0 else d)

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

      //标记变量
      val label = trimmed(r.size - 1).toInt

      //特征向量
      val features = trimmed.slice(4, r.size-1).map(d => if (d == "?") 0.0 else d.toDouble)

      LabeledPoint(label, Vectors.dense(features))

    }

    data.cache()

    data

  }


  /*************************************训练模型*************************************/
  /**
    * 选择使用的模型（除了决策树模型）
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


  /*************************************样例评估*************************************/
  /**
    * 使用模型预估样例
    */
  def useModelByOne(): Unit ={

    val data = getDataNormal()

    val dataPoint = data.first()

    println(dataPoint.features)

    val model = selectModel(0)

    val prediction = model.predict(dataPoint.features)

    println("prediction : "+prediction)

    println("trueLable : "+ dataPoint.label)

  }


  /*************************************模型性能评估*************************************/

  /**
    * 在二分类中,预测正确率可能是最简单评测方式,正确率等于训练样本中被正确分类的数目除以总样本数
    * 注：通过对输入特征进行预测并将预测值与实际标签进行比较,计算出模型在训练数据上的正确率。将对正确分类的样本数目求和并除以样本总数,得到平均分类正确率
    * @param way
    * @return
    */
  def checkModelByCorrect_Rate(way:Int): Double ={

    if(way == 1 || way ==0){

      val data = getDataNormal()

      val Model = selectModel(way)

      val TotalCorrect = data.map{
        point => if(Model.predict(point.features)== point.label) 1 else 0
      }.sum()

      val Accuracy = TotalCorrect / data.count()

      Accuracy

    } else if(way == 2){

      val data = getDataNotNegative()

      val Model = selectModel(way)

      val TotalCorrect = data.map{
        point => if(Model.predict(point.features)== point.label) 1 else 0
      }.sum()

      val Accuracy = TotalCorrect / data.count()

      Accuracy

    } else {

      val data = getDataNormal()

      val model = getDecisionTreeModel()

      val TotalCorrect = data.map{

        point =>

          val score = model.predict(point.features)

          val predicted = if(score > 0.5)1 else 0

          if(predicted == point.label) 1 else 0
      }.sum()

      val Accuracy = TotalCorrect / data.count()

      Accuracy
    }

  }


  /**
    * 准确率和召回率的检验
    * 准确率：真阳性除以阳性总数
    * 召回率：真阳性除以真阳性和假阴性的和
    * 真阳性率（TPR）：真阳性除以真阳性和假阴性的和;通常也称为敏感度
    * 假阳性率（FPR）：假阳性除以假阳性和真阴性的和
    * 真阳性：样本为1的本正确的估计成样本为1
    * 假阳性：样本为0的被错误的估计成样本为1
    * 真阴性：样本为0的被正确的估计成样本为0
    * 假阴性：样本为1的被错误的估计成样本为0
    * 注：准确率通常用于评价结果的质量,而召回率用来评价结果的完整性。
    * 准确率和召回率（PR）曲线：表示给定model随着阀值的改变，准确率和召回率的关系;PR下的面积为平均准确率
    * ROC曲线：对分类器的真阳性率和假阳性率的图形化解释
    * @param way
    * @return
    */
  def checkModelToROCAndPR(way:Int):Seq[(String, Double, Double)]={

    if(way == 0 ){

      val data = getDataNormal()

      val lrModel = selectModel(0)

      val lrMetrics:Seq[(String, Double, Double)] = Seq(lrModel).map{ model =>

        val scoreAndLabels = data.map{ point =>

          (model.predict(point.features),point.label)

        }

        val metrics = new BinaryClassificationMetrics(scoreAndLabels)

        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())

      }

      lrMetrics

    }else if(way ==1) {

      val data = getDataNormal()

      val svmModel = selectModel(1)

      val svmMetrics:Seq[(String, Double, Double)] = Seq(svmModel).map{ model =>

        val scoreAndLabels = data.map{ point =>

          (model.predict(point.features),point.label)

        }

        val metrics = new BinaryClassificationMetrics(scoreAndLabels)

        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())

      }

      svmMetrics

    }else if(way == 2){

      val nbData = getDataNotNegative()

      val nbModel = selectModel(2)

      val nbMetrics:Seq[(String, Double, Double)] = Seq(nbModel).map{ model =>

        val scoreAndLabels = nbData.map{ point =>

          (model.predict(point.features),point.label)

        }

        val metrics = new BinaryClassificationMetrics(scoreAndLabels)

        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())

      }

      nbMetrics

    } else {

      val data = getDataNormal()

      val dtModel = getDecisionTreeModel()

      val dtMetrcis:Seq[(String, Double, Double)] = Seq(dtModel).map{ model =>

        val scoreAndLabels = data.map{ point =>

          val score = model.predict(point.features)

          (if(score>0.5) 1.0 else 0.0, point.label)

        }

        val metrics = new BinaryClassificationMetrics(scoreAndLabels)

        (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())

      }

      dtMetrcis

    }

  }


  def checkModelByScaler(data:RDD[LabeledPoint]):Seq[(String,Double,Double)] = {

    val lrModel = LogisticRegressionWithSGD.train(data, numIterations)

    val lrMetrics:Seq[(String, Double, Double)] = Seq(lrModel).map{ model =>

      val scoreAndLabels = data.map{ point =>

        (model.predict(point.features),point.label)

      }

      val metrics = new BinaryClassificationMetrics(scoreAndLabels)

      (model.getClass.getSimpleName,metrics.areaUnderPR(),metrics.areaUnderROC())

    }

    lrMetrics
  }


  def checkModel(): Unit ={

    //展示正确率
    println("逻辑回归lr : " + checkModelByCorrect_Rate(0))

    println("线性向量机SVM : " + checkModelByCorrect_Rate(1))

    println("朴素贝叶斯 : " + checkModelByCorrect_Rate(2))

    println("决策树 ：" + checkModelByCorrect_Rate(3))

    //展示PR和ROC
    val allMetrcis = checkModelToROCAndPR(0) ++ checkModelToROCAndPR(1) ++ checkModelToROCAndPR(2) ++ checkModelToROCAndPR(3)

    allMetrcis.foreach{
      case (m,pr,roc) =>
        println(" (" + m +") : PR is "+pr +"  ROC is "+roc)
    }

  }

  /*************************************性能改进*************************************/

  def showmatrixSummary(data:RDD[LabeledPoint]): Unit ={

    val vectors = data.map(lp => lp.features)

    val matrix = new RowMatrix(vectors)

    //计算每列的汇总统计。
    val matrixSummary = matrix.computeColumnSummaryStatistics()

    print("均值")
    //均值
    println(matrixSummary.mean)

    println()

    print("最小值")

    //最小值
    println(matrixSummary.min)

    println()

    print("最大值")

    //最大值
    println(matrixSummary.max)

    println()

    print("方差")

    //方差
    println(matrixSummary.variance)

    println()

    print("非0项数目")
    //非0项数目
    println(matrixSummary.numNonzeros)

  }


  def checkScalerData(): Unit ={

    val data = getDataNormal()

    val vectors = data.map(lp =>lp.features)

    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)

    val scalerData = data.map(lp => LabeledPoint(lp.label,scaler.transform(lp.features)))

    val s1Point = checkModelToROCAndPR(0).apply(0)

    val s2Point = checkModelByScaler(scalerData).apply(0)

    println("标准化后 : PR--" + s1Point._2 +"   ROC--" + s1Point._3)

    println("标准化后 : PR--" + s2Point._2 +"   ROC--" + s2Point._3)

  }






}
