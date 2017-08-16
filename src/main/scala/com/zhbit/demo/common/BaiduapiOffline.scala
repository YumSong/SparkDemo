package com.zhbit.demo.common

import scala.collection.immutable.ListMap

/**
  * 百度地图坐标转化的实体工具类
  *
  * Created by admin on 2017/8/10.
  */
object BaiduapiOffline {
  private val pi: Double = 3.14159265358979324
  private val x_pi: Double = 3.14159265358979324 * 3000.0 / 180.0
  private val a: Double = 6378245.0
  private val ee: Double = 0.00669342162296594323

  //转换方法
  def transform(wgLat: Double, wgLon: Double): ListMap[String, Double] = {
    var mgLat: Double = .0
    var mgLon: Double = .0

    if (outOfChina(wgLat, wgLon)) {
      mgLat = wgLat
      mgLon = wgLon
      var map: ListMap[String, Double] = ListMap[String, Double]()
      map = map+("x"-> mgLat)
      map = map+("y"-> mgLon)
      return map
    }
    var dLat: Double = transformLat(wgLon - 105.0, wgLat - 35.0)
    var dLon: Double = transformLon(wgLon - 105.0, wgLat - 35.0)
    val radLat: Double = wgLat / 180.0 * pi
    var magic: Double = Math.sin(radLat)
    magic = 1 - ee * magic * magic
    val sqrtMagic: Double = Math.sqrt(magic)
    dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi)
    dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * pi)
    mgLat = wgLat + dLat
    mgLon = wgLon + dLon
    val map: ListMap[String, Double] = BaiduapiOffline.bd_encrypt(mgLat, mgLon)
    return map
  }

  private def outOfChina(lat: Double, lon: Double): Boolean = {
    if (lon < 72.004 || lon > 137.8347) return true
    if (lat < 0.8293 || lat > 55.8271) return true
    return false
  }

  private def transformLat(x: Double, y: Double): Double = {
    var ret: Double = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * Math.sqrt(Math.abs(x))
    ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(y * pi) + 40.0 * Math.sin(y / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * Math.sin(y / 12.0 * pi) + 320 * Math.sin(y * pi / 30.0)) * 2.0 / 3.0
    return ret
  }

  private def transformLon(x: Double, y: Double): Double = {
    var ret: Double = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * Math.sqrt(Math.abs(x))
    ret += (20.0 * Math.sin(6.0 * x * pi) + 20.0 * Math.sin(2.0 * x * pi)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(x * pi) + 40.0 * Math.sin(x / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * Math.sin(x / 12.0 * pi) + 300.0 * Math.sin(x / 30.0 * pi)) * 2.0 / 3.0
    return ret
  }

  //火星转百度
  def bd_encrypt(gg_lat: Double, gg_lon: Double): ListMap[String, Double] = {
    var bd_lat: Double = .0
    var bd_lon: Double = .0
    var map: ListMap[String, Double] = ListMap[String, Double]()
    val x: Double = gg_lon
    val y: Double = gg_lat
    val z: Double = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * x_pi)
    val theta: Double = Math.atan2(y, x) + 0.000003 * Math.cos(x * x_pi)
    bd_lon = z * Math.cos(theta) + 0.0065
    bd_lat = z * Math.sin(theta) + 0.006
    map = map+("x"-> bd_lon)
    map = map+("y"-> bd_lat)
    return map
  }
}
