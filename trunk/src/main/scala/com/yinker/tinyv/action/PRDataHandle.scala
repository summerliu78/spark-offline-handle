package com.yinker.tinyv.action

import com.yinker.tinyv.utils.HdfsUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/11/14.
  *com.yinker.tinyv.action.PRDataHandle
  */
object PRDataHandle {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("CallsHandle")
      .config("hive.auto.convert.join", "false")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.hive.convertMetastoreOrc", "false")
      .config("hive.mapjoin.localtask.max.memory.usage", "0.99")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val hdfs = new HdfsUtils()
    val sql = "select * from algo_tinyv.details_for_pr"
    val path = "/user/tinyv/test/lw/prdata"
    hdfs.pathExistDelete(path)
    HandleHiveData(spark, sql, path)


  }


  def HandleHiveData(spark: SparkSession, sql: String, savaPath: String) {
    val DF = spark.sql(sql)
    DF.rdd.map(x => {
      val m1 = x.getAs[String]("mobile")
      val m2 = x.getAs[String]("other_mobile")
      val m3 = x.getAs[String]("call_channel")
      if (m3 == "012001001") (m1, m2) else if (m3 == "012001002") (m2, m1) else ("-999", "-999")
    }).filter(x => {
      val m1 = x._1
      val m2 = x._2
      m1 != "-999" &&
        m1.startsWith("1") &&
        m2.startsWith("1") &&
        m1.length == 11 &&
        m2.length == 11
    })
      .distinct().saveAsTextFile(savaPath)
  }


}
