package com.yinker.tinyv.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by spark team on 2017/8/16.
  */

object DateUtils {

  /**
    * e.g getNowDate("yyyy-MM-dd HH:mm:ss")
    *
    * @param format like "yyyy-MM-dd HH:mm:ss"
    * @return String like "2017-08-01 12:59:26"
    */
  def getNowDate(format: String): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    dateFormat.format(now)
  }

  def unixTimeTotime(time: String): String = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = new Date(time.toLong)
    simpleDateFormat.format(date)
  }

  def timeToUnixTime(time: String): String = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = simpleDateFormat.parse(time)
    val ts = date.getTime
    String.valueOf(ts)
  }

  def getDaysInTwoDays(time1: String, time2: String): String = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date1 = simpleDateFormat.parse(time1)
    val ts1 = date1.getTime
    val num1 = String.valueOf(ts1).toLong
    val date2 = simpleDateFormat.parse(time2)
    val ts2 = date2.getTime
    val num2 = String.valueOf(ts2).toLong

    (math.abs(num1 - num2) / (1000 * 3600 * 24)).toString
  }


  /**
    *
    * @return return CurrentTime
    */
  def getCurrentTime: Long = new Date().getTime.toString.substring(0, 10).toLong

  /**
    *
    */
  def getTimeDiffForHours(thisTime: Long, thatTime: Long): Long = {
    (thisTime - thatTime) / (1000 * 60 * 60)
  }


  /**
    *
    * e.g  getBeforeDay("yyyy-MM-dd HH:mm:ss",10)  //获取十天前的日期格式
    *
    * @param format     like "yyyy-MM-dd HH:mm:ss"
    * @param betweenNum like "10"
    * @return String like "2017-08-01 12:59:26"
    */
  def getBeforeDay(format: String, betweenNum: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, s"-$betweenNum".toInt)
    dateFormat.format(cal.getTime)
  }


  def getBeforeDayForUnixTime(startTime: String, betweenNum: Int): String = {
    // 时间表示格式可以改变，yyyyMMdd需要写例如20160523这种形式的时间
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    // 将字符串的日期转为Date类型，ParsePosition(0)表示从第一个字符开始解析
    val date = sdf.parse(startTime)
    //    println(date)
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    // add方法中的第二个参数n中，正数表示该日期后n天，负数表示该日期的前n天
    calendar.add(Calendar.DATE, -betweenNum)
    val date1 = calendar.getTime
    val out = sdf.format(date1)
    timeToUnixTime(out)
  }


  def getUnixTimeBeforeDaysToTime(time: String, beforeDays: Int): String = {
    getBeforeDayForUnixTime(unixTimeTotime(time), beforeDays)
  }

  /**
    * 将yyyy-MM-dd HH:mm:ss换成数值,本月的为0,上月的为1,以此类推,六月以上均为6
    *
    * @param timeStr yyyy-MM-dd HH:mm:ss
    * @return 整型数值
    */


  def dateMapping2Int(timeStr: String): Int = {
    val cal = Calendar.getInstance()
    val nowMonth = cal.get(Calendar.MONTH)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.setTime(format.parse(timeStr))
    val oldMonth = cal.get(Calendar.MONTH)
    nowMonth - oldMonth match {
      case 0 => 0
      case 1 => 1
      case 2 => 2
      case 3 => 3
      case 4 => 4
      case 5 => 5
      case _ => 6
    }
  }

  def main(args: Array[String]): Unit = {
    //    println(unixTimeTotime(getUnixTimeBeforeDaysToTime("1505707200000", 1)).replaceAll(" [0-9]+:[0-9]+:[0-9]+$", ""))
    //    println(timeToUnixTime("2017-09-18 12:00:00"))


    println(getDaysInTwoDays("2015-01-01", "2015-03-01"))


  }

}
