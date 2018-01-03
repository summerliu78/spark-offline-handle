package com.yinker.tinyv.process

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by moses on 2017/9/7.
  */
class JLXContactCount() extends Serializable{
  private var spark: SparkSession = null

  def this(spark: SparkSession) {
    this()
    this.spark = spark
  }

  /*


  * 贷后邦索伦_间接联系人在黑名单的数量 0000343
  * */
  def readDataFromHive(): RDD[(String, String)] = {
    val sqlPlatform =
      s"""
         |select n.user_id,m.blackcount,n.ds from
         |(select platformchannel,mobile,case when
         |rawdata.report_data.user_info_check.check_black_info['contacts_class2_blacklist_cnt']='' then '0'
         |when rawdata.report_data.user_info_check.check_black_info['contacts_class2_blacklist_cnt']=null then '0'
         |when rawdata.report_data.user_info_check.check_black_info['contacts_class2_blacklist_cnt']='NULL' then '0'
         |else rawdata.report_data.user_info_check.check_black_info['contacts_class2_blacklist_cnt']
         |end blackcount
         |from bdw_tinyv_outer.b_fuse_jxl_mongo_i
         |where dt = date_add(current_date(),-1)) m join
         |(select user_id,ds,mobile from  ods_tinyv.o_fuse_user_info_contrast_d info where dt = date_add(current_date(),-1)) n
         |on m.platformchannel=n.ds and m.mobile=n.mobile
       """.stripMargin
    val platformDF = spark.sql(sqlPlatform)
    platformDF.printSchema()
    val all = platformDF.rdd.map(row => (row.getAs[Int]("user_id") + "_" + row.getAs[String]("ds"), row.getAs[String]("blackcount")))
    all
  }


  def totalAmt(): RDD[(String, String)] = {

    val sql =
      s"""
         |select n.user_id,n.ds,m.total_amt_avg from
         |(select mobile,ds,sum(total_amt)/count(1) total_amt_avg from bdw_tinyv_outer.b_fuse_bill_detail_i
         |where dt<=date_add(current_date(),-1)
         |group by mobile,ds ) m join
         |(select user_id,ds,mobile from  ods_tinyv.o_fuse_user_info_contrast_d info where dt = date_add(current_date(),-1)) n
         |on n.ds=m.ds and n.mobile=m.mobile
       """.stripMargin
    val totalAmt = spark.sql(sql).rdd.map { row =>
      val avg = row.getAs[Double]("total_amt_avg")
      val average = f"$avg%1.2f"
      (row.getAs[Int]("user_id") + "_" + row.getAs[String]("ds"), average)
    }
    totalAmt
  }
}

object JLXContactCount {

  def apply(): JLXContactCount = new JLXContactCount()

  def apply(spark: SparkSession): JLXContactCount = new JLXContactCount(spark)

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .enableHiveSupport()
      .appName("JLX")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.hive.convertMetastoreOrc", "false")
      .config("hive.mapjoin.localtask.max.memory.usage", "0.99")
      .getOrCreate()
    println("开始执行了，集群千万别挂啊。。。。")
    val flag=args(0)
    val result=if(flag=="1")JLXContactCount(spark).readDataFromHive() else JLXContactCount(spark).totalAmt()
      result.saveAsTextFile(args(1))


  }

}
