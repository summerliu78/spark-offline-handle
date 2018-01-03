package com.yinker.tinyv.action

import com.yinker.tinyv.utils.{DateUtils, ModelArgs, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by think on 2017/9/11.
  */
object CallsEntrance {


  val LOG: Logger = Logger.getLogger(ModelEntrance.getClass)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val modelAgs = ModelArgs(args)
    val jobType = modelAgs.jobType
    val dateUtils = DateUtils


    val conf = SparkUtil.getSparkConf(jobType)
    val sc = new SparkContext(conf)
    val ctx = new HiveContext(sc)
    ctx.setConf("hive.auto.convert.join", "false")
    ctx.setConf("spark.sql.hive.convertMetastoreOrc", "false")
    ctx.setConf("hive.mapjoin.localtask.max.memory.usage", "0.99")

//    val callsPro = CallDetailProsser(ctx,sc)


  }
}
