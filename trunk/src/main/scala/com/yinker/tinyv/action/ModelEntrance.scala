package com.yinker.tinyv.action

import com.yinker.tinyv.process.DataProcessor
import com.yinker.tinyv.utils.{DateUtils, ModelArgs, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by spark-team on 2017/8/15.
  */
object ModelEntrance {

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

    modelAgs.jobScheduler match {
      case "rely" => DataProcessor.runRely(sc, ctx, modelAgs.jobScale, modelAgs.jobType)
      case "merge" => DataProcessor.runMerge(sc, ctx, modelAgs.jobScale)
      case "union" => DataProcessor.runUnionIncre(ctx)
      case "join" => DataProcessor.runFinalJoin(ctx,modelAgs.jobScale)
    }

    LOG.info(s"任务 《${modelAgs.jobScheduler}》 执行完成 时间 <${dateUtils.getNowDate("yyyy-MM-dd HH:mm:ss")}>")

  }
}
