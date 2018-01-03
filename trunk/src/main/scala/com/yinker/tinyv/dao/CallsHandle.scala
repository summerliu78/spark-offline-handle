package com.yinker.tinyv.dao

import com.yinker.tinyv.process.CallDetailProsser
import com.yinker.tinyv.process.common.TestData
import com.yinker.tinyv.utils.{DataUtils, DateUtils, HdfsUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/9/1.
  */
object CallsHandle {
  val hdfsUtils = new HdfsUtils()
  val LOG = Logger.getLogger(this.getClass)

  def runSaveDataToHdfs(spark: SparkSession, flag: Boolean = false): Unit = {


    //    val comm = CallDetailsCommon

    val test = TestData


    /**
      * 通话详单详情
      *
      * 最小粒度 --> mobile
      *
      * 所有爬取渠道的最新一次爬取该手机号的记录
      *
      */
    val creditInfoRdd: RDD[
      (
        String, //mobile 1
          String, //  user_id 2
          String, //  channel 3
          String, //  creditId 4
          String, //  creditTime 5
          String, //creditStatus 6
          Array[(String, String, String)], //  List[mobile,CallingOrCalled，callTime] 7
          Array[String], //  List[mobile] 8
          Array[String], //  List[mobile] 9
          Array[String]) //  List[mobile] 10
      ] = test.creditInfoRdd(spark.sparkContext)

    /**
      * 全量还款事件详情
      */
    //  val loanInfoRDD: RDD[UserCreditInfo] = null
    //                        mobile  userId  channel  loanId ,loanCreateTime,dueDay,overDueDay
    val userRepayInfoRDD: RDD[(String, (String, String, String, String, String, Int))] = test.userRepayInfoRDD(spark.sparkContext)
    //


    /**
      * 全量借款事件详情
      */
    //                    mobile userId   channel createTime
    val loanInfoRDD: RDD[(String, String, String, String)] = test.loanInfoRDD(spark.sparkContext)
    //


    /**
      *
      * @return 取出通话详单  通讯录及其二者对应银行卡手机号和该手机号不同的部分的并集
      */


    val overDueDayComm = CallDetailProsser.overDueDayCommon(userRepayInfoRDD, creditInfoRdd).cache()
    val pass30AvgRate = DataUtils.pass30AvgRate(spark.sparkContext, "C:\\Users\\think\\Downloads\\aaaa.txt")

    val passRatioInLastMonth = CallDetailProsser.passRatioInLastMonth(creditInfoRdd, pass30AvgRate)
    passRatioInLastMonth.foreach(println)
    val date = DateUtils.getNowDate("yyyy-MM-dd")
    //1
    //k ，(mobile 通讯录中手机号个数)
    //k   userId_Channel
    //    LOG.info("开始存入HDFS...")
    //    val hdfsPath = "/user/tinyv/model/calls/center_data"
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/nonredundantCallNum")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/nonredundantCallNum")
    //    nonredundantCallNumRDD.saveAsTextFile(s"$hdfsPath/$date/nonredundantCallNum")
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/maxOverdueDaysInCallsAndContacts")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/maxOverdueDaysInCallsAndContacts")
    //    maxOverdueDaysInCallsAndContactsRDD.saveAsTextFile(s"$hdfsPath/$date/maxOverdueDaysInCallsAndContacts")
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/overdueUsersAbove7days")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/overdueUsersAbove7days")
    //    overdueUsersAbove7daysRDD.saveAsTextFile(s"$hdfsPath/$date/overdueUsersAbove7days")
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/successfulLoansInLast30days")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/successfulLoansInLast30days")
    //    successfulLoansInLast30daysRDD.saveAsTextFile(s"$hdfsPath/$date/successfulLoansInLast30days")
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/stableContract3mthCount")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/stableContract3mthCount")
    //    stableContract3mthCountRDD.saveAsTextFile(s"$hdfsPath/$date/stableContract3mthCount")
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/appliedUsersInLast7days")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/appliedUsersInLast7days")
    //    appliedUsersInLast7daysRDD.saveAsTextFile(s"$hdfsPath/$date/appliedUsersInLast7days")
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/c180ExhaleCountHalfyear")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/c180ExhaleCountHalfyear")
    //    c180ExhaleCountHalfyearRDD.saveAsTextFile(s"$hdfsPath/$date/c180ExhaleCountHalfyear")
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/maxOverdueDaysInCallsAndContacts")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/maxOverdueDaysInCallsAndContacts")
    //    maxOverdueDaysInCallsAndContacts.saveAsTextFile(s"$hdfsPath/$date/maxOverdueDaysInCallsAndContacts")
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/passRatioInLastMonth")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/passRatioInLastMonth")
    //
    //    passRatioInLastMonth.saveAsTextFile(s"$hdfsPath/$date/passRatioInLastMonth")
    //
    //    val resRDD = maxOverdueDaysInCallsAndContacts
    //    nonredundantCallNumRDD
    //      //      //                最大逾期天数  3
    //      .join(maxOverdueDaysInCallsAndContactsRDD)
    //      //      //    七天以上逾期率
    //      .join(overdueUsersAbove7daysRDD)
    //      //      //30天内成功放款人数
    //      .join(successfulLoansInLast30daysRDD)
    //      //      //三个月以上呼叫人数
    //      .join(stableContract3mthCountRDD)
    //      //      //过去七天申请当前平台人数
    //      .join(appliedUsersInLast7daysRDD)
    //      //      //六个月的呼出数量
    //      .join(c180ExhaleCountHalfyearRDD)
    //      //    通讯录与通话记录重合手机号最大逾期天数
    //      .join(maxOverdueDaysInCallsAndContacts)
    //      //    过去1个月通讯录/通话记申请客户的通过率除以过去一个月平均通过率
    //      .join(passRatioInLastMonth)
    //      .map(_.toString().replaceAll("[()]", ""))
    //    if (hdfsUtils.dirExists(s"$hdfsPath/$date/result")) hdfsUtils.pathExistDelete(s"$hdfsPath/$date/result")
    //    resRDD.saveAsTextFile(s"$hdfsPath/$date/result")

    //    val path = s"E:/project/data/res-${DateUtils.timeToUnixTime(DateUtils.getNowDate("yyyy-MM-dd HH:mm:ss"))}"
    //    hdfsUtils.pathExistDelete(path)
    //    resRDD.foreach(println)
    //    .coalesce(1).saveAsTextFile(path)


  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //    if (args.length <= 2) {
    //      LOG.info("请输入参数，如果args(0)=sauron则执行SauronProcessor类里的方法，args(0)=jxl执行JLXContactCount类里的方法，执行否则执行CallsHandle类")
    //      System.exit(0)
    //    }
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("CallsHandle")
      .master("local[*]")
      //      .config("hive.auto.convert.join", "false")
      //      .config("spark.some.config.option", "some-value")
      //      .config("spark.sql.hive.convertMetastoreOrc", "false")
      //      .config("hive.mapjoin.localtask.max.memory.usage", "0.99")
      //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    //    val method = args(1)
    //    val path = args(2)
    //    if (args(0) == "sauron") {
    //      LOG.info("开始执行sauron里面的方法....")
    //      if (method.toUpperCase() == "union117".toUpperCase()) SauronProcessor.union117(spark).saveAsTextFile(path)
    //      if (method.toUpperCase() == "emerContactCount".toUpperCase()) SauronProcessor.emerContactCount(spark).saveAsTextFile(path)
    //      if (method.toUpperCase() == "compSauronDF".toUpperCase()) SauronProcessor.compSauronDF(spark).saveAsTextFile(path)
    //      if (method.toUpperCase() == "compTDloan".toUpperCase()) SauronProcessor.compTDloan(spark).saveAsTextFile(path)
    //    } else if (args(0) == "default") {
    //      LOG.info("开始执行handle... 需要传入args(1)=true 或者不传入" + method + " " + path)
    //      val flag = if (args(1) == "true") args(1).toBoolean else false
    //      runSaveDataToHdfs(spark, flag)
    //    } else if (args(0) == "jxl") {
    //      LOG.info("开始执行jxl里面的方法....")
    //      if (method.toUpperCase() == "readDataFromHive".toUpperCase()) JLXContactCount(spark).readDataFromHive().saveAsTextFile(path)
    //      if (method.toUpperCase() == "totalAmt".toUpperCase()) JLXContactCount(spark).totalAmt().saveAsTextFile(path)
    //    }


    CallsHandle.runSaveDataToHdfs(spark)
  }


}
