package com.yinker.tinyv.utils

import org.apache.log4j.Logger
import org.apache.spark.SparkConf

/**
  * Spark工具类，负责处理Spark Conf的获取、注册Kryo Classes等。
  *
  */
object SparkUtil {
  val LOG: Logger = Logger.getLogger(SparkUtil.getClass)

  def getSparkConf[E](applicationName: String): SparkConf = {

    val sparkConf = new SparkConf().setAppName(applicationName)
    val sparkMaster = System.getProperty("spark.master")
    LOG.info(s"sparkMaster:$sparkMaster")
    if (sparkMaster == null) {
      LOG.info("Using local[*] as spark master since spark.master is not set.")
      sparkConf.setMaster("local[*]")
    }
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(
      classOf[com.yinker.tinyv.utils.ModelArgs],
      classOf[com.yinker.tinyv.utils.PathArgs],
      classOf[com.yinker.tinyv.utils.QianHai],
      classOf[com.yinker.tinyv.utils.HdfsUtils]
    ))
    sparkConf
  }
}