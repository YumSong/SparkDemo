package com.zhbit.spark.common

import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool

object ConnetionInfo {

  private val URL = "jdbc:mysql://192.168.0.223:3306/vlpr?useUnicode=true&characterEncoding=utf8"

  private val NEW_URL = "jdbc:mysql://192.168.0.236:3306/vlpr?useUnicode=true&characterEncoding=utf8"

  private val USERNAME = "root"

  private val PASSWORD = "root"

  private val DRIVER = "com.mysql.jdbc.Driver"

  private var SPARK_URL = "spark://datanode1:7077"

  private val EXECUTOR_MEMORY = "spark.executor.memory"

  private val DRIVER_MEMORY = "spark.driver.memory"

  private val CORE_MAX = "spark.cores.max"

  private var JAR_PATH = "/home/song/IdeaProjects/SparkDemo/out/artifacts/SparkDemo_jar/SparkDemo.jar"

  private val redisHost = "192.168.0.236"

  private val redisPort = 6379

  private val redisTimeout = 30000

  private val APP_NAME = "Test"

  lazy val jedisPool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  def getSc(appName:String): SparkContext ={

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(ConnetionInfo.SPARK_URL)
      .set(ConnetionInfo.EXECUTOR_MEMORY, "1g")
//      .set(ConnetionInfo.DRIVER_MEMORY, "1g")
      .set(ConnetionInfo.CORE_MAX,"1")
      .setJars(List(ConnetionInfo.JAR_PATH))

    val sc = new SparkContext(conf)

    sc
  }

  def getSc(): SparkContext ={

    val conf = new SparkConf()
      .setAppName(APP_NAME)
      .setMaster(ConnetionInfo.SPARK_URL)
      .set(ConnetionInfo.EXECUTOR_MEMORY, "1g")
      .set(ConnetionInfo.CORE_MAX,"1")
      .setJars(List(ConnetionInfo.JAR_PATH))

    val sc = new SparkContext(conf)

    sc
  }

  def getProp(): Properties ={

    val properties = new Properties()

    properties.put("user",ConnetionInfo.USERNAME)

    properties.put("driver",ConnetionInfo.DRIVER)

    properties.put("password",ConnetionInfo.PASSWORD)

    properties
  }

  def getURL(): String = {URL}

  def getNewURL(): String = { NEW_URL}

  def setJar(path :String) ={ this.JAR_PATH = path}

  def getJar() = this.JAR_PATH

  def setMaster(URL:String) = { this.SPARK_URL = URL}

}