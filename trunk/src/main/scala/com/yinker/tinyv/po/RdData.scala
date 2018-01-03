package com.yinker.tinyv.po

import com.yinker.tinyv.utils.{DateUtils, HdfsUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/10/26.
  */
object RdData {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("CallsHandle")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val callsRDD = sc.textFile("/user/tinyv/test/lw/txl.txt").distinct()
    val JXL_RDD = sc.textFile("/user/tinyv/test/lw/jxl.txt").distinct()
    val info = sc.textFile("/user/tinyv/test/lw/rd.txt").distinct()
    val ageAndGender = sc.textFile("/user/tinyv/test/lw/user_id-ds-user_age-user_gender.txt").distinct()


    val hdfs = new HdfsUtils
    hdfs.pathExistDelete("/user/tinyv/test/lw/ress")


    val JQXX = ageAndGender.filter(_.split(",").length == 4).map(x => {
      val s = x.split(",")
      //userId_Channel age gender
      (s"${s(0)}_${s(1)}", (s(2), s(3)))
    })


    val JXL = JXL_RDD.filter(x => {
      val s = x.split(",").length
      s >= 4 && s <= 11
    }).map(x => {


//      orderid
//      ,userid
//      ,datatype
//      ,ds channel
//      ,result['indirect_contacts_blacklist_cnt'] indirect_contacts_blacklist_cnt
//      ,result['xyklhmlxqk_check_point_cn_r'] xyklhmlxqk_check_point_cn_r
//      ,result['jlgypjdxsl_sms_cnt'] jlgypjdxsl_sms_cnt
//      ,result['jlgypjhcsc_call_out_time'] jlgypjhcsc_call_out_time
//      ,result['contacts_router_ratio'] contacts_router_ratio
//      ,result['contact_morning_cnt'] contact_morning_cnt
//      ,result['direct_contacts_blacklist_cnt'] direct_contacts_blacklist_cnt
      val s = x.split(",")
      val orderid = s(0)
      val userid = s(1)
      val datatype = s(2)
      val channel = s(3)


      val indirect_contacts_blacklist_cnt = if (s.length != 11) "" else s(4)
      val xyklhmlxqk_check_point_cn_r = if (s.length != 11) "" else s(5)
      val jlgypjdxsl_sms_cnt = if (s.length != 11) "" else s(6)
      val jlgypjhcsc_call_out_time = if (s.length != 11) "" else s(7)
      val contacts_router_ratio = if (s.length != 11) "" else s(8)
      val contact_morning_cnt = if (s.length != 11) "" else s(9)
      val direct_contacts_blacklist_cnt = if (s.length != 11) "" else s(10)

      (
        s"${orderid}_${userid}_$channel",
        (indirect_contacts_blacklist_cnt,
          xyklhmlxqk_check_point_cn_r,
          jlgypjdxsl_sms_cnt,
          jlgypjhcsc_call_out_time,
          contacts_router_ratio,
          contact_morning_cnt,
          direct_contacts_blacklist_cnt))
    })


    val calls = callsRDD.filter(x => {
      val s = x.split(",").length
      s <= 16 && s >= 11
    }).map(x => {
//      loanid
//      ,userid
//      ,type
//      ,userphone
//      ,useridcard
//      ,createtimestr
//      ,datatype
//      ,load_time
//      ,ds channel
//      ,result['nonredundantCallNum'] nonredundantCallNum
//      ,result['appliedUsersInLast7days'] appliedUsersInLast7days
//      ,result['successfulLoansInLast30days'] successfulLoansInLast30days
//      ,result['overdueUsersAbove7days'] overdueUsersAbove7days
//      ,result['maxOverdueDaysInCallsAndContacts'] maxOverdueDaysInCallsAndContacts


      val s = x.split(",")
      val loanid = s(0)
      val userid = s(1)
      val Type = s(2)
      val userphone = s(3)
      val useridcard = s(4)
      val createtimestr = s(5)
      val datatype = s(7)
      val load_time = s(8) + " " + s(9)
      val channel = s(10)

      val nonredundantCallNum = if (s.length != 16) "" else s(11)
      val appliedUsersInLast7days = if (s.length != 16) "" else s(12)
      val successfulLoansInLast30days = if (s.length != 16) "" else s(13)
      val overdueUsersAbove7days = if (s.length != 16) "" else s(14)
      val maxOverdueDaysInCallsAndContacts = if (s.length != 16) "" else s(15)
      (
        s"${loanid}_${userid}_$channel",
        (
          nonredundantCallNum,
          appliedUsersInLast7days,
          successfulLoansInLast30days,
          overdueUsersAbove7days,
          maxOverdueDaysInCallsAndContacts,
          s"${userid}_$channel",
          createtimestr
        ))
    })

    val joinRDD = calls.join(JXL)
      .map(x => {
        val k = x._2._1._6
        val nonredundantCallNum = x._2._1._1
        val appliedUsersInLast7days = x._2._1._2
        val successfulLoansInLast30days = x._2._1._3
        val overdueUsersAbove7days = x._2._1._4
        val maxOverdueDaysInCallsAndContacts = x._2._1._5
        val createTime = x._2._1._7

        val indirect_contacts_blacklist_cnt = x._2._2._1
        val xyklhmlxqk_check_point_cn_r = x._2._2._2
        val jlgypjdxsl_sms_cnt = x._2._2._3
        val jlgypjhcsc_call_out_time = x._2._2._4
        val contacts_router_ratio = x._2._2._5
        val contact_morning_cnt = x._2._2._6
        val direct_contacts_blacklist_cnt = x._2._2._7
        (
          k, (
          x._1,
          indirect_contacts_blacklist_cnt,
          xyklhmlxqk_check_point_cn_r,
          jlgypjdxsl_sms_cnt,
          jlgypjhcsc_call_out_time,
          contacts_router_ratio,
          contact_morning_cnt,
          direct_contacts_blacklist_cnt,
          nonredundantCallNum,
          appliedUsersInLast7days,
          successfulLoansInLast30days,
          overdueUsersAbove7days,
          maxOverdueDaysInCallsAndContacts,
          createTime))
      }).join(JQXX).map(x => {
      (
        x._2._1._1,
        (
          x._2._1._14,
          x._2._1._2,
          x._2._1._3,
          x._2._1._4,
          x._2._1._5,
          x._2._1._6,
          x._2._1._7,
          x._2._1._8,
          x._2._1._9,
          x._2._1._10,
          x._2._1._11,
          x._2._1._12,
          x._2._1._13,
          x._2._2._1,
          x._2._2._2
        )
      )
    })

    val infoRDD = info.filter(_.split(",").length == 14).map(x => {
      val s = x.split(",")
      val k = s"${s(0)}_${s(1)}_${s(2)}"
      (k, (s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10), s(11), s(12), s(13)))
    })
      .combineByKey(
        List(_),
        (x: List[(String, String, String, String, String, String, String, String, String, String, String)],
         y: (String, String, String, String, String, String, String, String, String, String, String)) => y +: x,
        (x: List[(String, String, String, String, String, String, String, String, String, String, String)],
         y: List[(String, String, String, String, String, String, String, String, String, String, String)]) => x ++ y)
      .map(x => {
        //        var xMap = new scala.collection.mutable.HashMap[String, String]()
        var a1 = ""
        var a2 = ""
        var a3 = ""
        var a4 = ""
        var a5 = ""
        var a6 = ""
        var a7 = ""
        var a8 = ""
        var a9 = ""
        var a10 = ""
        var a11 = ""
        x._2.map(y => {
          if (y._1.trim != "" && y._1.nonEmpty && y._1.trim != "NULL") a1 = y._1
          if (y._2.trim != "" && y._2.nonEmpty && y._2.trim != "NULL") a2 = y._2
          if (y._3.trim != "" && y._3.nonEmpty && y._3.trim != "NULL") a3 = y._3
          if (y._4.trim != "" && y._4.nonEmpty && y._4.trim != "NULL") a4 = y._4
          if (y._5.trim != "" && y._5.nonEmpty && y._5.trim != "NULL") a5 = y._5
          if (y._6.trim != "" && y._6.nonEmpty && y._6.trim != "NULL") a6 = y._6
          if (y._7.trim != "" && y._7.nonEmpty && y._7.trim != "NULL") a7 = y._7
          if (y._8.trim != "" && y._8.nonEmpty && y._8.trim != "NULL") a8 = y._8
          if (y._9.trim != "" && y._9.nonEmpty && y._9.trim != "NULL") a9 = y._9
          if (y._10.trim != "" && y._10.nonEmpty && y._10.trim != "NULL") a10 = y._10
          if (y._11.trim != "" && y._11.nonEmpty && y._11.trim != "NULL") a11 = y._11
          1
        })

        //        a7是时间
        (x._1, (a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11))
      })
    joinRDD.join(infoRDD).map(x => {

      val timeDiff = if (x._2._1._1.trim == "" || x._2._1._1.isEmpty || x._2._1._1 == "NULL" || x._2._2._7.trim == "" || x._2._2._7.trim == "NULL" || x._2._2._7.isEmpty) "" else DateUtils.getDaysInTwoDays(x._2._1._1, x._2._2._7)

      s"${x._1},${x._2._1._2},${x._2._1._3},${x._2._1._4},${x._2._1._5}," +
        s"${x._2._1._6},${x._2._1._7},${x._2._1._8},${x._2._1._9},${x._2._1._10}," +
        s"${x._2._1._11},${x._2._1._12},${x._2._1._13},${x._2._1._14},${x._2._1._15}," +
        s"${x._2._2._1},${x._2._2._2},${x._2._2._3},${x._2._2._4},${x._2._2._5}," +
        s"${x._2._2._6},$timeDiff,${x._2._2._8},${x._2._2._9},${x._2._2._10},${x._2._2._11}"
      //      (x._1,
      //        x._2._1._2,
      //        x._2._1._3,
      //        x._2._1._4,
      //        x._2._1._5,
      //        x._2._1._6,
      //        x._2._1._7,
      //        x._2._1._8,
      //        x._2._1._9,
      //        x._2._1._10,
      //        x._2._1._11,
      //        x._2._1._12,
      //        x._2._1._13,
      //        x._2._1._14,
      //        x._2._1._15,
      //
      //        x._2._2._1,
      //        x._2._2._2,
      //        x._2._2._3,
      //        x._2._2._4,
      //        x._2._2._5,
      //        x._2._2._6,
      //        timeDiff,
      //        x._2._2._8,
      //        x._2._2._9,
      //        x._2._2._10,
      //        x._2._2._11
      //      )
    })
      .coalesce(1).saveAsTextFile("/user/tinyv/test/lw/ress")

  }




}
