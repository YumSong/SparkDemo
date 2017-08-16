package com.zhbit.demo.common

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON

import scala.collection.immutable.{List, ListMap}
import scala.io.Source

/**
  * Created by admin on 2017/8/16.
  */
object HttpUtil {

  def changeGps2Redis(): List[ListMap[String,String]] ={

    var url:String = ""

    var last_get_time:String = ""

    if(last_get_time!=null){

      url = url+"last_get_time="+delStr(last_get_time)

    }

    val str = Source.fromURL(url,"UTF-8")

    val j1 = JSON.parseObject(str.mkString)

    val j2 = JSON.parseArray(j1.get("value").toString)

    last_get_time = j1.get("server_time").toString

    val iterator = j2.iterator()

    //保存到jedis里面的list
    var list = List[ListMap[String,String]]()

    //保存到list里面的元素
    var listMap = ListMap[String,String]()

    while (iterator.hasNext ==true){

      val json = JSON.parseObject(iterator.next().toString)

      val driverlat:Double = json.get("latitude").toString.toDouble

      val driverlng:Double = json.get("longitude").toString.toDouble

      listMap = listMap+("latitude"->BaiduapiOffline.transform(driverlat,driverlng).get("y").toString)

      listMap = listMap+("longitude"->BaiduapiOffline.transform(driverlat,driverlng).get("x").toString)

      listMap = listMap+("address"->json.get("address").toString)

      listMap = listMap+("device_id"->json.get("device_id").toString)

      listMap = listMap+("direction"->json.get("direction").toString)

      listMap = listMap+("gps_time"->json.get("gps_time").toString)

      listMap = listMap+("speed"->json.get("speed").toString)

      listMap = listMap+("zhuang_tai"->json.get("zhuang_tai").toString)

      list = list :+ listMap

    }

    list

  }

  def delStr(str:String): String ={

    val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd%20HH:mm:ss")

    var date:Date = null

    date = dateFormat1.parse(str)

    dateFormat2.format(date)

  }
}
