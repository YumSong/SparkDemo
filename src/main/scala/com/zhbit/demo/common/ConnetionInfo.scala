package com.zhbit.demo.common

import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.JedisPool

object ConnetionInfo {

  private val URL = "jdbc:mysql://192.168.0.223:3306/vlpr?useUnicode=true&characterEncoding=utf8"

  private val NEW_URL = "jdbc:mysql://192.168.0.236:3306/vlpr?useUnicode=true&characterEncoding=utf8"

  private val USERNAME = "root"

  private val PASSWORD = "root"

  private val TABLE = "runcarinfo"

  private val DRIVER = "com.mysql.jdbc.Driver"

  private val SPARK_URL = "spark://datanode1:7077"

  private val JAR_PATH = "/home/song/IdeaProjects/SparkDemo/out/artifacts/SparkDemo_jar/SparkDemo.jar"

  private val redisHost = "192.168.0.236"

  private val redisPort = 6379

  private val redisTimeout = 30000

  lazy val jedisPool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout)

  def getSc(): SparkContext ={

    val conf = new SparkConf().setAppName("Spark F Mysql").setMaster(ConnetionInfo.SPARK_URL)
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

}
