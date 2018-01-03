package com.yinker.tinyv.process.common

import com.yinker.tinyv.utils.HdfsUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/9/6.
  */
object CallDetailsCommon {
  val hdfsUitls = new HdfsUtils

  /*
  * 、所有申请授信用户的最新一次申请事件（不区分渠道） 对应爬取的 （(通话详单，通话发生时间)a+通讯录 b）c 的 信息，
       如果 c 在平台注册过且注册手机号和银行卡预留手机号不同，取出 c  对应的银行卡预留手机号 a->d ,  b->e，
       a , b , d , e union 后得到结果
       *
    *
    * @param hiveContext
    * @return mobile,userId,channel,callDettails (mobile callingOrCalled time),contacts,calldetailsBankPhone,contactBankPhone
    */
  def createContacts(spark: SparkSession, path: String,flag:Boolean=false): RDD[(String, String, String, Array[(String, String, String)], Array[String], Array[String], Array[String])] = {
    val credit_call_detail_v1Drop =
      s"""
         |drop table if exists algo_tinyv.credit_call_detail_v1
      """.stripMargin
    val credit_call_detail_v1SQL =
      s"""
         |create table algo_tinyv.credit_call_detail_v1 as
         |select
         |	task_id
         |	,ds
         |	,case when length(other_mobile)=11 and substr(other_mobile,1,1) = '1' then other_mobile
         |		  when length(other_mobile)=13 and (substr(other_mobile,1,3) = '861' or substr(other_mobile,1,3) = '001') then substr(other_mobile,3,13)
         |		  when length(other_mobile)=14 and (length(other_mobile) = 14 and substr(other_mobile,1,4) = '+861') then substr(other_mobile,4,13)
         |		  when length(other_mobile)=15 and substr(other_mobile,1,5) = '00861' then substr(other_mobile,5,13)
         |		  else other_mobile
         |		  end other_mobile
         |	,call_channel
         |	,call_datetime
         |from bdw_tinyv_outer.b_fuse_call_detail_i
         |where
         |	dt <= date_add(current_date(),-1)
       """.stripMargin
    val credit_contact_v1Drop =
      s"""
         |drop table if exists algo_tinyv.credit_contact_v1
       """.stripMargin
    val credit_contact_v1SQL =
      s"""
         |create table algo_tinyv.credit_contact_v1 as
         |select
         |	user_id
         |	,ds
         |	,phones contacts_mobile
         |from
         |	(select
         |		userid user_id
         |		,'weibo' ds
         |		,rawdatas.phone phone
         |	from ods_tinyv_outer.weibo_sina_vcard_mongo_i
         |	lateral view explode(rawdata) a as rawdatas
         |	where
         |		dt <= date_add(current_date(),-1)
         |	) a
         |lateral view explode(split(a.phone,',')) cm as phones
         |union
         |select
         |    user_id
         |    ,ds
         |	,contacts_phone1 contacts_mobile
         |from ods_tinyv.o_fuse_user_contacts_d
         |where
         |	dt = date_add(current_date(),-1)
         |	and contacts_phone1 is not null
         |union
         |select
         |    user_id
         |    ,ds
         |	,contacts_phone2 contacts_mobile
         |from ods_tinyv.o_fuse_user_contacts_d
         |where
         |	dt = date_add(current_date(),-1)
         |	and contacts_phone2 is not null
         |union
         |select
         |    user_id
         |    ,ds
         |	,contacts_phone3 contacts_mobile
         |from ods_tinyv.o_fuse_user_contacts_d
         |where
         |	dt = date_add(current_date(),-1)
         |	and contacts_phone3 is not null
         |union
         |select
         |    user_id
         |    ,ds
         |	,contacts_phone4 contacts_mobile
         |from ods_tinyv.o_fuse_user_contacts_d
         |where
         |	dt = date_add(current_date(),-1)
         |	and contacts_phone4 is not null
         |union
         |select
         |    user_id
         |    ,ds
         |	,contacts_phone5 contacts_mobile
         |from ods_tinyv.o_fuse_user_contacts_d
         |where
         |	dt = date_add(current_date(),-1)
         |	and contacts_phone5 is not null
       """.stripMargin
    val reserve_mobile_v1Drop =
      s"""
         |drop table if exists algo_tinyv.reserve_mobile_v1
       """.stripMargin
    val reserve_mobile_v1SQL =
      s"""
         |create table algo_tinyv.reserve_mobile_v1 as
         |select
         |	mobile
         |	,collect_set(reserve_mobile) reserve_mobile
         |from ods_tinyv.o_fuse_user_info_contrast_d
         |where
         |	dt = date_add(current_date(),-1)
         |	and mobile != reserve_mobile
         |group by mobile
       """.stripMargin
    val credit_info_v1Drop =
      s"""
         |drop table if exists algo_tinyv.credit_info_v1
       """.stripMargin
    val credit_info_v1SQL =
      s"""
         |create table algo_tinyv.credit_info_v1 as
         |select
         |	d.mobile mobile
         |	,d.user_id user_id
         |	,d.ds ds
         |	,d.task_id task_id
         |from
         |	(select
         |		a.user_id
         |		,a.ds
         |		,b.mobile
         |		,c.task_id
         |		,row_number() over (partition by b.mobile order by a.apply_time desc) rn
         |	from
         |		(select
         |			aa.user_id
         |			,aa.ds
         |			,aa.carrier_id
         |			,aa.apply_time
         |		from
         |			(select
         |				user_id
         |				,ds
         |				,carrier_id
         |				,apply_time
         |				,row_number() over (partition by user_id,ds order by apply_time desc) rn1
         |			from edw_tinyv.e_credit_apply_detail_d
         |			where
         |				dt = date_add(current_date(),-1)
         |				and apply_status != '001002001'
         |			) aa
         |		where aa.rn1 = 1
         |		) a
         |	left join
         |		(select
         |			user_id
         |			,ds
         |			,mobile
         |		from ods_tinyv.o_fuse_user_info_contrast_d
         |		where
         |			dt = date_add(current_date(),-1)
         |		) b
         |	on a.user_id = b.user_id and a.ds = b.ds
         |	left join
         |		(select
         |			id
         |			,ds
         |			,task_id
         |		from bdw_tinyv.b_fuse_user_carrier_d
         |		where
         |			dt = date_add(current_date(),-1)
         |		) c
         |	on a.carrier_id = c.id and a.ds = c.ds
         |	) d
         |where
         |	d.rn = 1
       """.stripMargin
    val result1_v1Drop =
      s"""
         |drop table if exists algo_tinyv.result1_v1
       """.stripMargin
    val result1_v1SQL =
      s"""
         |create table algo_tinyv.result1_v1 as
         |select
         |	a.mobile mobile
         |	,a.user_id user_id
         |	,a.ds ds
         |	,collect_list(b.other) call_detail_list
         |from algo_tinyv.credit_info_v1 a
         |left join
         |	(select
         |		task_id
         |		,ds
         |		,concat('(',other_mobile,';',call_channel,';',call_datetime,')') other
         |	from algo_tinyv.credit_call_detail_v1
         |	) b
         |on a.task_id = b.task_id and a.ds = b.ds
         |group by a.mobile,a.user_id,a.ds
       """.stripMargin
    val result2_v1Drop =
      s"""
         |drop table if exists algo_tinyv.result2_v1
       """.stripMargin
    val result2_v1SQL =
      s"""
         |create table algo_tinyv.result2_v1 as
         |select
         |	a.mobile mobile
         |	,a.user_id user_id
         |	,a.ds ds
         |	,collect_list(concat_ws(',',b.reserve_mobile)) reserve_mobile_list1
         |from algo_tinyv.credit_info_v1 a
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
         |		from algo_tinyv.credit_call_detail_v1
         |		where
         |			length(other_mobile) = 11
         |			and substr(other_mobile,-1) != '.'
         |			and substr(other_mobile,1,2) in ('13','14','15','17','18')
         |		group by task_id,ds,other_mobile
         |		) x
         |	left join algo_tinyv.reserve_mobile_v1 y
         |	on x.other_mobile = y.mobile
         |	) b
         |on a.task_id = b.task_id and a.ds = b.ds
         |group by a.mobile,a.user_id,a.ds
       """.stripMargin
    val result3_v1Drop =
      s"""
         |drop table if exists algo_tinyv.result3_v1
         """.stripMargin
    val result3_v1SQL =
      s"""
         |create table algo_tinyv.result3_v1 as
         |select
         |	a.mobile mobile
         |	,a.user_id user_id
         |	,a.ds ds
         |	,b.contacts_mobile_list contacts_mobile_list
         |from algo_tinyv.credit_info_v1 a
         |left join
         |	(select
         |		user_id
         |		,ds
         |		,collect_list(contacts_mobile) contacts_mobile_list
         |	from algo_tinyv.credit_contact_v1
         |	group by user_id,ds
         |	) b
         |on a.user_id = b.user_id and a.ds = b.ds
       """.stripMargin
    val result4_v1Drop =
      s"""
         |drop table if exists algo_tinyv.result4_v1
         """.stripMargin
    val result4_v1SQL =
      s"""
         |create table algo_tinyv.result4_v1 as
         |select
         |	a.mobile mobile
         |	,a.user_id user_id
         |	,a.ds ds
         |	,collect_list(concat_ws(',',b.reserve_mobile)) reserve_mobile_list2
         |from algo_tinyv.credit_info_v1 a
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
         |		from algo_tinyv.credit_contact_v1
         |		where
         |			length(contacts_mobile) = 11
         |			and substr(contacts_mobile,-1) != '.'
         |			and substr(contacts_mobile,1,2) in ('13','14','15','17','18')
         |		group by user_id,ds,contacts_mobile
         |		) x
         |	left join algo_tinyv.reserve_mobile_v1 y
         |	on x.contacts_mobile = y.mobile
         |	) b
         |on a.user_id = b.user_id and a.ds = b.ds
         |group by a.mobile,a.user_id,a.ds
       """.stripMargin
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
         |from algo_tinyv.result1_v1 a
         |left join algo_tinyv.result2_v1 b
         |on a.mobile = b.mobile
         |left join algo_tinyv.result3_v1 c
         |on a.mobile = c.mobile
         |left join algo_tinyv.result4_v1 d
         |on a.mobile = d.mobile
       """.stripMargin
    if(flag){
      spark.sql(credit_call_detail_v1Drop)
      spark.sql(credit_call_detail_v1SQL)
      spark.sql(credit_contact_v1Drop)
      spark.sql(credit_contact_v1SQL)
      spark.sql(reserve_mobile_v1Drop)
      spark.sql(reserve_mobile_v1SQL)
      spark.sql(credit_info_v1Drop)
      spark.sql(credit_info_v1SQL)
      spark.sql(result1_v1Drop)
      spark.sql(result1_v1SQL)
      spark.sql(result2_v1Drop)
      spark.sql(result2_v1SQL)
      spark.sql(result3_v1Drop)
      spark.sql(result3_v1SQL)
      spark.sql(result4_v1Drop)
      spark.sql(result4_v1SQL)
      System.exit(0)
    }

    val resDF = spark.sql(resultSQL)
    /*if (hdfsUitls.dirExists(path)) {
      hdfsUitls.pathExistDelete(path)
    }
    resDF.write.format("parquet").save(path)*/
    println("createContacts=================================")
    resDF.show()
    resDF.printSchema()
    val result = resDF.rdd.filter{row=>
      val call_detail_list = row.getSeq[(String)](3).toArray
      val filteredDetail=for(detail <- call_detail_list if(detail!="")) yield detail
      val reserve_mobile_list = row.getSeq[String](4).toArray
      var filteredReverseMobile=for(mobile <-reserve_mobile_list if(mobile!="")) yield mobile

      reserve_mobile_list!="NULL"||filteredReverseMobile.length!=0||reserve_mobile_list!="NULL"||filteredDetail.length!=0
    }.map { row =>

      val call_detail_list = row.getSeq[(String)](3).toArray.map{line=>
        val array=line.replaceAll("(","").replaceAll(")","").split(",")
        (array(0),array(1),(array(2)))
      }
      val reserve_mobile_list1 = row.getSeq[String](4).toArray
      val contacts_mobile_list = row.getSeq[String](5).toArray
      val reserve_mobile_list2 = row.getSeq[String](6).toArray
      val mobile = row.getAs[String]("mobile")
      val user_id = row.getAs[Int]("user_id")+""
      val ds = row.getAs[String]("ds")

      (mobile, user_id, ds, call_detail_list, reserve_mobile_list1, contacts_mobile_list, reserve_mobile_list2)
    }
    result
  }

  /*
  * 全量有有还款表现的借款事件（dueDay 在昨天之前）
  * */

  def loanEvent(spark: SparkSession, path: String): RDD[(String, (String, String, String, String, String, Int))] = {
    val sql =
      s"""
         |select
         |	b.mobile mobile
         |	,a.user_id user_id
         |	,a.ds channel
         |	,a.loan_id loan_id
         |	,a.create_time create_time
         |	,a.overdue_day overdue_day
         |	,a.due_day due_day
         |	,c.stage_sum stage_sum
         |from
         |	(select
         |		loan_id
         |		,user_id
         |		,ds
         |		,create_time
         |		,collect_list(overdue_day) overdue_day
         |		,collect_list(due_day) due_day
         |	from edw_tinyv.e_repay_plan_detail_d
         |	where
         |		dt = date_add(current_date(),-1)
         |		and (to_date(due_day) <= date_add(current_date(),-1)
         |			 or
         |			 repay_status = '002005002')
         |	group by loan_id,user_id,ds,create_time
         |	) a
         |left join
         |	(select
         |		user_id
         |		,ds
         |		,mobile
         |	from ods_tinyv.o_fuse_user_info_contrast_d
         |	where
         |		dt = date_add(current_date(),-1)
         |	) b
         |on a.user_id = b.user_id and a.ds = b.ds
         |left join
         |	(select
         |		loan_id
         |		,ds
         |		,stage_sum
         |	from edw_tinyv.e_loan_detail_info_d
         |	where
         |		dt = date_add(current_date(),-1)
         |	) c
         |on a.loan_id = c.loan_id and a.ds = c.ds
     """.stripMargin
    val resDF = spark.sql(sql)
    println("loanEvent=========================================")
    resDF.show()
    resDF.printSchema()
    /*if (hdfsUitls.dirExists(path)) {
      hdfsUitls.pathExistDelete(path)
    }
    resDF.write.format("parquet").save(path)*/
    val result = resDF.rdd.map { row =>
      val mobile = row.getAs[String]("mobile")
      val user_id = row.getAs[Int]("user_id")+""
      val channel = row.getAs[String]("channel")
      val loan_id = row.getAs[String]("loan_id")
      val create_time = row.getAs[String]("create_time")
      val overdue_day = row.getSeq[Int](5)(0)
      val due_day = row.getSeq[String](6)(0)
      (mobile, (user_id, channel, loan_id, create_time, due_day, overdue_day))
    }
    result

  }

  /*
* 全量授信信息(分渠道，相同手机号在不同渠道申请授信算多次
* */
  def wholeCredit(spark: SparkSession, path: String): RDD[(String, (String, String, String, String))] = {
    val sql =
      s"""
         |select
         |	b.mobile mobile
         |	,a.user_id user_id
         |	,a.ds channel
         |	,a.apply_time apply_time
         |  ,case when a.apply_status = '001002004' then 0
         |        else 1
         |        end apply_status
         |from edw_tinyv.e_credit_apply_detail_d a
         |left join
         |	(select
         |		user_id
         |		,ds
         |		,mobile
         |	from ods_tinyv.o_fuse_user_info_contrast_d
         |	where
         |		dt = date_add(current_date(),-1)
         |	) b
         |on a.user_id = b.user_id and a.ds = b.ds
         |where
         |	a.dt = date_add(current_date(),-1)
         |	and a.apply_status != '001002001'
       """.stripMargin
    val resDF = spark.sql(sql)
    println("wholeCredit=================================")
    resDF.show()
    resDF.printSchema()
    /*if (hdfsUitls.dirExists(path)) {
      hdfsUitls.pathExistDelete(path)
    }
    resDF.write.format("parquet").save(path)*/
    val result = resDF.rdd.map { row =>
      val mobile = row.getAs[String]("mobile")
      val user_id = row.getAs[Int]("user_id")+""
      val channel = row.getAs[String]("channel")
      val apply_time = row.getAs[String]("apply_time")
      val status = row.getAs[String]("apply_status")
      (mobile, (user_id, channel, apply_time, status))
    }
    result
  }

  /*
  *全量借款成功事件（loan_id + channel）
  * */
  def wholeLoanSuccsessEvent(spark: SparkSession, path: String): RDD[(String, String, String, String)] = {
    val sql =
      s"""
         |select
         |	b.mobile mobile
         |	,a.user_id user_id
         |	,a.ds channel
         |	,a.loan_time loan_time
         |from bdw_tinyv.b_fuse_loan_record_d a
         |left join
         |	(select
         |		user_id
         |		,ds
         |		,mobile
         |	from ods_tinyv.o_fuse_user_info_contrast_d
         |	where
         |		dt = date_add(current_date(),-1)
         |	) b
         |on a.user_id = b.user_id and a.ds = b.ds
         |where
         |	a.dt = date_add(current_date(),-1)
       """.stripMargin
    val resDF = spark.sql(sql)
    println("wholeLoanSuccsessEvent===============================")
    resDF.show()
    resDF.printSchema()
    /*if (hdfsUitls.dirExists(path)) {
      hdfsUitls.pathExistDelete(path)
    }
    resDF.write.format("parquet").save(path)*/
    val result = resDF.rdd.map { row =>
      val mobile = row.getAs[String]("mobile")
      val user_id = row.getAs[Int]("user_id")+""
      val channel = row.getAs[String]("channel")
      val loan_time = row.getAs[String]("loan_time")
      (mobile, user_id, channel, loan_time)
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("CallsHandle")
      .config("hive.auto.convert.join", "false")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.hive.convertMetastoreOrc", "false")
      .config("hive.mapjoin.localtask.max.memory.usage", "0.99")
      .getOrCreate()

  }

}
