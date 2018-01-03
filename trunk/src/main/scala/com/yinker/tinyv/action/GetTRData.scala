package com.yinker.tinyv.action

import java.time.LocalDateTime

import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/11/24.
  *com.yinker.tinyv.action.GetTRData
  */
object GetTRData {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport().getOrCreate()

    // 参数形式“path=/home/reynold/Documents/complexnet/temp”
    val savePath = args(0).split("=")(1)

    val current_day = LocalDateTime.now().toString().substring(0, 10)
    val pass_day = LocalDateTime.now().minusDays(1).toString().substring(0, 10)
    import spark.implicits._
    val hdfs = new com.yinker.tinyv.utils.HdfsUtils()
    hdfs.pathExistDelete(s"$savePath/$pass_day")
    val mobileCreditSql =
      s"""
         |select
         |b.MOBILE as MOBILE,
         |max(FIRST_APPLY_LOAN_SUCC_TIME) as FIRST_APPLY_LOAN_SUCC_TIME,
         |max(MAX_OVERDUE_DAY_CNT) as MAX_OVERDUE_DAY_CNT
         |from
         |(select USER_ID,ds,FIRST_APPLY_CREDIT_SUCC_TIME,FIRST_APPLY_LOAN_SUCC_TIME,MAX_OVERDUE_DAY_CNT
         |from
         |edw_tinyv.e_user_integ_info_d
         |where dt='$pass_day' and FIRST_APPLY_CREDIT_SUCC_TIME is not null) a
         |join
         |(select USER_ID,ds,MOBILE from ods_tinyv.o_fuse_user_info_contrast_d
         |where dt='$pass_day') b
         |on a.USER_ID=b.USER_ID and a.ds=b.ds group by b.MOBILE
      """.stripMargin

    // 通过授信的mobile
    val passCredit = spark.sql(mobileCreditSql).cache()

    // 过滤出通过授信但是没有借款的mobile,过滤条件为: 首次下单通过时间为空
    passCredit.filter(row => row.get(1) == null)
      .map(_.getAs[String](0)).coalesce(1).distinct()
      .write.text(savePath + s"/$pass_day/applycreditNoBorrow")

    // 过滤出通过授信有借款行为的mobile
    val borrow = passCredit.filter(row => row.get(1) != null).cache()

    // 在borrow基础上,过滤出历史最大逾期天数为7到15天的mobile ==> 借款但是不属于白名单也不属于黑名单
    borrow.filter(row => row.getAs[Int](2) > 7 && row.getAs[Int](2) <= 14)
      .map(_.getAs[String](0)).coalesce(1).distinct()
      .write.text(savePath + s"/$pass_day/borrowNotwb")

    // 在borrow基础上,过滤出历史最大逾期天数小于等于7天的mobile ==> 白名单
    borrow.filter(row => row.getAs[Int](2) <= 7)
      .map(_.getAs[String](0)).coalesce(1).distinct()
      .write.text(savePath + s"/$pass_day/whitelist")

    // 在borrow基础上,过滤出历史最大逾期天数超过15天的mobile ==> 黑名单
    borrow.filter(row => row.getAs[Int](2) > 14)
      .map(_.getAs[String](0)).coalesce(1).distinct()
      .write.text(savePath + s"/$pass_day/blacklist")

    passCredit.createOrReplaceTempView("passCredit")
    //
    //    val callDetailSql =
    //      s"""
    //         |select
    //         |b.mobile as mobile,
    //         |b.other_mobile as other_mobile,
    //         |b.call_channel as call_channel,
    //         |b.call_time as call_time
    //         |from passCredit
    //         |join
    //         |(select mobile,other_mobile,call_channel,call_time from bdw_tinyv_outer.b_fuse_call_detail_i
    //         |where dt < '$current_day') b
    //         |on passCredit.mobile=b.mobile
    //      """.stripMargin
    //
    //    val a = spark.sql(callDetailSql).map(row => {
    //      val mobile = row.getAs[String](0)
    //      val other_mobile = row.getAs[String](1)
    //      val callPattern = "^[0-9]+".r
    //      val call_time_raw = row.getAs[String](3)
    //      val call_time = call_time_raw match {
    //        case callPattern(_*) => call_time_raw.toDouble
    //        case _ => 0.0
    //      }
    //      row.getAs[String](2) match {
    //        case "012001001" => (mobile + "," + other_mobile + ",CALL_OUT", call_time)
    //        case "012001002" => (other_mobile + "," + mobile + ",CALL_OUT", call_time)
    //      }
    //    }).filter(x => {
    //      val phonePattern = "^1[0-9]{10},1[0-9]{10},CALL_OUT".r
    //      x._1 match {
    //        case phonePattern(_*) => true
    //        case _ => false
    //      }
    //    }).rdd.combineByKey(
    //      call_time => (1, call_time),
    //      (c1: (Int, Double), newCallTime) => (c1._1 + 1, c1._2 + newCallTime),
    //      (c1: (Int, Double), c2: (Int, Double)) => (c1._1 + c2._1, c1._2 + c2._2))
    //      .map { case (name, (num, callTime)) => (name, num, callTime / num) }
    //      .filter(_._2 > 1) // 过滤出通话次数大于１的
    //      .map(x => x._1 + "," + x._2 + "," + x._3.formatted("%.2f"))
    //      .saveAsTextFile(savePath + s"/$pass_day/relationship")

    val callDetailSql1 =
      s"""
         |select
         |  c.mobile as mobile
         |  ,c.other_mobile as other_mobile
         |  ,c.call_channel as call_channel
         |from
         |  (select
         |      b.mobile as mobile
         |      ,b.other_mobile as other_mobile
         |      ,b.call_channel as call_channel
         |      ,b.call_time as call_time
         |   from
         |      passCredit
         |      join
         |   (select
         |      mobile
         |      ,other_mobile
         |      ,call_channel
         |      ,call_time
         |   from
         |     bdw_tinyv_outer.b_fuse_call_detail_i
         |   where
         |     dt < '$current_day') b
         |   on passCredit.mobile=b.mobile) c
         |join
         |passCredit on passCredit.mobile=c.other_mobile""".stripMargin

    val call_detail_in_credit = spark.sql(callDetailSql1).map(x => {
      val mobile = x.getAs[String]("mobile")
      val other_mobile = x.getAs[String]("other_mobile")
      val call_channel = x.getAs[String]("call_channel")
      call_channel match {
        case "012001001" => mobile + "," + other_mobile
        case "012001002" => other_mobile + "," + mobile
      }
    }).filter(x => {
      val phonePattern = "^1[0-9]{10},1[0-9]{10}".r
      x match {
        case phonePattern(_*) => true
        case _ => false
      }
    }).rdd.distinct()
    println("--------------rdd count --------" + call_detail_in_credit.count())
    call_detail_in_credit.saveAsTextFile(savePath + s"/$pass_day/detailInfo")
  }
}