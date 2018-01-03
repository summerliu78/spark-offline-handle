package com.yinker.tinyv.dao

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Created by ThinkPad on 2017/9/14.
  */
object CallDetailsCreditInfo {
  private val result1_v1SQL =
    s"""
       |select
       |	a.mobile mobile
       |	,a.user_id user_id
       |	,a.ds ds
       |	,collect_list(b.other) call_detail_list
       |from credit_info_v1 a
       |left join
       |	(select
       |		task_id
       |		,ds
       |		,concat(other_mobile,',',call_channel,',',call_datetime) other
       |	from credit_call_detail_v1
       |	) b
       |on a.task_id = b.task_id and a.ds = b.ds
       |group by a.mobile,a.user_id,a.ds
       """.stripMargin
  private val result2_v1SQL =
    s"""
       |select
       |	a.mobile mobile
       |	,a.user_id user_id
       |	,a.ds ds
       |	,collect_list(concat_ws(',',b.reserve_mobile)) reserve_mobile_list1
       |from credit_info_v1 a
       |left join
       |	(select
       |		x.task_id
       |		,x.ds
       |		,y.reserve_mobile
       |	from
       |		(select
       |			task_id
       |			,ds
       |			,other_mobile
       |		from credit_call_detail_v1
       |		where
       |			length(other_mobile) = 11
       |			and substr(other_mobile,-1) != '.'
       |			and substr(other_mobile,1,2) in ('13','14','15','17','18')
       |		group by task_id,ds,other_mobile
       |		) x
       |	left join reserve_mobile_v1 y
       |	on x.other_mobile = y.mobile
       |	) b
       |on a.task_id = b.task_id and a.ds = b.ds
       |group by a.mobile,a.user_id,a.ds
     """.stripMargin
  private val result3_v1SQL =
    s"""
       |select
       |	a.mobile mobile
       |	,a.user_id user_id
       |	,a.ds ds
       |	,b.contacts_mobile_list contacts_mobile_list
       |from credit_info_v1 a
       |left join
       |	(select
       |		user_id
       |		,ds
       |		,collect_list(contacts_mobile) contacts_mobile_list
       |	from credit_contact_v1
       |	group by user_id,ds
       |	) b
       |on a.user_id = b.user_id and a.ds = b.ds
     """.stripMargin
  private val result4_v1SQL =
    s"""
       |select
       |	a.mobile mobile
       |	,a.user_id user_id
       |	,a.ds ds
       |	,collect_list(concat_ws(',',b.reserve_mobile)) reserve_mobile_list2
       |from credit_info_v1 a
       |left join
       |	(select
       |		x.user_id
       |		,x.ds
       |		,y.reserve_mobile
       |	from
       |		(select
       |			user_id
       |			,ds
       |			,contacts_mobile
       |		from credit_contact_v1
       |		where
       |			length(contacts_mobile) = 11
       |			and substr(contacts_mobile,-1) != '.'
       |			and substr(contacts_mobile,1,2) in ('13','14','15','17','18')
       |		group by user_id,ds,contacts_mobile
       |		) x
       |	left join reserve_mobile_v1 y
       |	on x.contacts_mobile = y.mobile
       |	) b
       |on a.user_id = b.user_id and a.ds = b.ds
       |group by a.mobile,a.user_id,a.ds
     """.stripMargin

  def test(sc: SparkContext, hiveContext: HiveContext): RDD[(String, String, String, Array[(String, String, String)], Array[String], Array[String], Array[String])] = {

    hiveContext.setConf("hive.auto.convert.join", "false")
    hiveContext.setConf("spark.sql.hive.convertMetastoreOrc", "false")
    hiveContext.setConf("hive.mapjoin.localtask.max.memory.usage", "0.99")
    val credit_call_detail_v1Schema = StructType(Array(StructField("task_id", DataTypes.StringType, false),
      StructField("ds", DataTypes.StringType, false),
      StructField("other_mobile", DataTypes.StringType, false),
      StructField("call_channel", DataTypes.StringType, false), StructField("call_datetime", DataTypes.StringType, false)
    ))
    val credit_call_detail_v1RDD = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\data\\credit_call_detail_v1.csv").map(_.split(",")).filter(x => x.length == 5 && x(4) != "NULL").map { fields =>
      val createTime = fields(4)
      val form = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = form.parse(createTime).getTime + ""
      Row(fields(0), fields(1), fields(2), fields(3), date)
    }
    hiveContext.createDataFrame(credit_call_detail_v1RDD, credit_call_detail_v1Schema).registerTempTable("credit_call_detail_v1")
    val credit_info_v1Schema = StructType(Array(StructField("mobile", DataTypes.StringType, false),
      StructField("user_id", DataTypes.StringType, false), StructField("ds", DataTypes.StringType, false),
      StructField("task_id", DataTypes.StringType, false)))
    val credit_info_v1RDD = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\data\\credit_info_v1.csv").map { line =>
      val fields = line.split(",")
      Row(fields(0), fields(1), fields(2), fields(3))
    }

    hiveContext.createDataFrame(credit_info_v1RDD, credit_info_v1Schema).registerTempTable("credit_info_v1")
    val credit_contact_v1Schema = StructType(Array(StructField("user_id", DataTypes.StringType, true),
      StructField("ds", DataTypes.StringType, true), StructField("contacts_mobile", DataTypes.StringType, true)))
    val credit_contact_v1RDD = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\data\\credit_contact_v1.csv").map { line =>
      val fields = line.split(",")
      if (fields.length > 2) {
        Row(fields(0), fields(1), fields(2))
      }
      else Row(fields(0), fields(1), "")

    }
    hiveContext.createDataFrame(credit_contact_v1RDD, credit_contact_v1Schema).registerTempTable("credit_contact_v1")
    val reserve_mobile_v1Schema = StructType(Array(StructField("mobile", DataTypes.StringType, true), StructField("reserve_mobile", DataTypes.createArrayType(DataTypes.StringType), true)))
    val reserve_mobile_v1RDD = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\data\\reserve_mobile_v1.csv").map { line =>
      val fields = line.split(",")
      val course = for (i <- 1 to fields.length - 1) yield fields(i)
      Row(fields(0), course)
    }
    hiveContext.createDataFrame(reserve_mobile_v1RDD, reserve_mobile_v1Schema).registerTempTable("reserve_mobile_v1")
    hiveContext.sql(result1_v1SQL).registerTempTable("result1_v1")
    hiveContext.sql(result2_v1SQL).registerTempTable("result2_v1")
    hiveContext.sql(result3_v1SQL).registerTempTable("result3_v1")
    hiveContext.sql(result4_v1SQL).registerTempTable("result4_v1")
    val resultSQL =
      s"""
         |select
         |	a.mobile mobile
         |	,a.user_id user_id
         |	,a.ds channel
         |	,a.call_detail_list call_detail_list
         |	,b.reserve_mobile_list1 reserve_mobile_list1
         |	,c.contacts_mobile_list contacts_mobile_list
         |	,d.reserve_mobile_list2 reserve_mobile_list2
         |from result1_v1 a
         |left join result2_v1 b
         |on a.mobile = b.mobile
         |left join result3_v1 c
         |on a.mobile = c.mobile
         |left join result4_v1 d
         |on a.mobile = d.mobile
       """.stripMargin
    val result = hiveContext.sql(resultSQL).rdd.map { row =>

      val call_detailstr = if (row.getSeq[(String)](3) != null) row.getSeq[(String)](3).toArray else Array.empty[(String)]
      var call_detail_list = Array.empty[(String, String, String)]
      if (call_detailstr.length > 0) {
        call_detail_list = call_detailstr.map(x => x.split(",")).map((x => (x(0), x(1), x(2))))
      }
      val reserve_mobile_list1 = if (row.getSeq[String](4) != null) row.getSeq[String](4).toArray else Array.empty[String]
      val contacts_mobile_list = if (row.getSeq[String](5) != null) row.getSeq[String](5).toArray else Array.empty[String]
      val reserve_mobile_list2 = if (row.getSeq[String](6) != null) row.getSeq[String](6).toArray else Array.empty[String]
      val mobile = row.getAs[String]("mobile")
      val user_id = row.getAs[String]("user_id")
      val ds = row.getAs[String]("channel")
      (mobile, user_id, ds, call_detail_list, reserve_mobile_list1, contacts_mobile_list, reserve_mobile_list2)
    }
    result
  }

  def loanEvent(sc: SparkContext, hiveContext: HiveContext): RDD[(String, (String, String, String, String, String, Int))] = {
    val res = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\data\\loanevent.csv").map(_.split(",")).filter(x => x.length == 7 && x(4) != "NULL").map { fields =>
      val createTime = fields(4).replaceAll("..$", "")
      val form = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = form.parse(createTime)
      (fields(0), (fields(1), fields(2), fields(3), date.getTime + "", fields(6), fields(5).toInt))
    }
    res
  }

  def wholeCredit(sc: SparkContext, hiveContext: HiveContext): RDD[(String, (String, String, String, String))] = {
    val res = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\data\\wholeCredit.csv").map(_.split(",")).filter(x => x.length == 5 && x(3) != "NULL").map { fields =>
      val createTime = fields(3).replaceAll("..$", "")
      val form = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = form.parse(createTime)
      (fields(0), (fields(1), fields(2), date.getTime + "", fields(4)))
    }
    res
  }

  def wholeLoanSuccsessEvent(sc: SparkContext, hiveContext: HiveContext): RDD[(String, String, String, String)] = {
    //mobile  user_id channel creditTime
    val res = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\data\\wholeLoanSuccsessEvent.csv").map(_.split(",")).filter(x => x.length == 4 && x(3) != "NULL").map { fields =>
      val createTime = fields(3).replaceAll("..$", "")
      val form = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = form.parse(createTime)
      (fields(0), fields(1), fields(2), date.getTime + "")
    }
    res
  }

  def calldetail(sc: SparkContext): RDD[(String, String, String, Array[(String, String, String)], Array[String], Array[String], Array[String])] = {
    //mobile,user_id,channel,call_detail_list,reserve_mobile_list1,contacts_mobile_list,reserve_mobile_list2
    val res = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\data\\test.csv").map(_.split("==")).map { line =>
      val mobileuseridchannel = line(0)
      val unic = mobileuseridchannel.split(",")
      val mobile = unic(0)
      val userId = unic(1)
      val channel = unic(2)
      val call = line(1).substring(1, line(1).length - 1)
      val reserve1 = if (line(2) != "NULL") line(2).substring(1, line(2).length - 1) else "NULL"
      val contacts = if (line(3) != "NULL") line(3).substring(1, line(3).length - 1) else "NULL"
      val reserve2 = if (line(4) != "NULL") line(4).substring(1, line(4).length - 1) else "NULL"
      val call_detail_list = call.split(",").map { elem =>
        val param = elem.substring(1, elem.length - 1).split(";")
        val form = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val time = form.parse(param(2)).getTime + ""
        (param(0), param(1), time)
      }
      var reserve_mobile_list1 = Array.empty[String]
      var contacts_mobile_list = Array.empty[String]
      var reserve_mobile_list2 = Array.empty[String]
      if (reserve1 != "NULL" && !reserve1.split(",").isEmpty) reserve_mobile_list1 = reserve1.split(",")
      if (contacts != "NULL" && !contacts.split(",").isEmpty) contacts_mobile_list = reserve1.split(",")
      if (reserve2 != "NULL" && !reserve2.split(",").isEmpty) reserve_mobile_list2 = reserve1.split(",")
      (mobile,userId,channel,call_detail_list,reserve_mobile_list1,contacts_mobile_list,reserve_mobile_list2)
    }
    res
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext(new SparkConf().setAppName("aaaa").setMaster("local[*]"))
    val ctx = new HiveContext(sc)
    ctx.setConf("hive.auto.convert.join", "false")
    ctx.setConf("spark.sql.hive.convertMetastoreOrc", "false")
    ctx.setConf("hive.mapjoin.localtask.max.memory.usage", "0.99")
    //test(sc, ctx).saveAsTextFile("D:\\data\\1")
    calldetail(sc).foreach(println(_))

  }
}
