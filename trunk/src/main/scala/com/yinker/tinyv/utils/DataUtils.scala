package com.yinker.tinyv.utils

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by spark team on 2017/8/15.
  */
object DataUtils extends Serializable {
  val LOG: Logger = Logger.getLogger(DataUtils.getClass)

  private val hdfsUtils = new HdfsUtils

  def writeParquet(sc: SparkContext,
                   ctx: HiveContext,
                   sqlFilePath: String,
                   savePath: String): Boolean = {
    var sql = ""


    if (hdfsUtils.dirExists(sqlFilePath)) {
      sc.textFile(sqlFilePath).collect.foreach(x => sql += x.trim + " ")
    }
    else {
      LOG.error(s"${DateUtils.getNowDate("yyyy-MM-dd HH:mm:ss")} sql文件 < $sqlFilePath > 不存在")
    }
    println(s"This ctx is $ctx")
    //    if (sqlFilePath.replaceAll("/.*/", "") == "load_150_info") {
    //      ctx.sql("drop table if exist tinyv_analysis_db.loan_call_detail_150_model")
    ////                        tinyv_analysis_db.loan_call_detail_150_model
    //    }
    val df = ctx.sql(sql)
    hdfsUtils.pathExistDelete(savePath)
    df.write.mode(SaveMode.Append).parquet(savePath)
    df.unpersist()
    hdfsUtils.getFileStatus(savePath)
  }

  /**
    *
    * @param appendOrOverwrite true 为Append false 为 OverWrite
    * @param dataFrame         传入的df
    * @param path              save的路径
    */
  def dataFrameWriteToParquet(appendOrOverwrite: Boolean, dataFrame: DataFrame, path: String) {

    if (dataFrame != null && path != "" && dataFrame.count() != 0) {

      if (appendOrOverwrite) {
        dataFrame.write.mode(SaveMode.Append).parquet(path)
      } else {
        dataFrame.write.mode(SaveMode.Overwrite).parquet(path)
      }
      LOG.info(s"----------------${dataFrame.getClass.getName} 写入 <$path> 成功")

    } else {
      LOG.error("DF 为 null 或 path 为空  或 df count 为0")
    }
  }

  /**
    *
    * @param appendOrOverwrite true 为Append false 为 OverWrite
    * @param dataFrame         传入的df
    * @param path              save的路径
    */
  def dataFrameWriteToJson(appendOrOverwrite: Boolean, dataFrame: DataFrame, path: String): Unit = {
    if (dataFrame != null && path != "" && dataFrame.count() != 0) {
      if (!hdfsUtils.dirExists(path)) {
        if (appendOrOverwrite) {
          dataFrame.write.mode(SaveMode.Append).json(path)
        } else {
          dataFrame.write.mode(SaveMode.Overwrite).json(path)
        }
        LOG.info(s"df <${dataFrame.getClass.getName}> json格式写入，目录<$path> 成功")
      } else {
        if (appendOrOverwrite) {
          dataFrame.write.mode(SaveMode.Append).json(path)
        } else {
          dataFrame.write.mode(SaveMode.Overwrite).json(path)

        }
        LOG.info(s"df <${dataFrame.getClass.getName}> json格式写入，目录<$path> 成功")
      }
    }
  }

  /**
    * 前海的sql(load_qianhai_info.sql)跑不出来结果，用hive -f的方式
    *
    * @param sc       SparkContext
    * @param hdfsPath 使用hive -f跑出前海数据在hdfs上的路径
    * @return
    */
  def readQianHaiDF(sc: SparkContext, hdfsPath: String): Option[DataFrame] = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    if (hdfsPath != "" || hdfsPath != null) {

      if (hdfsUtils.dirExists(hdfsPath)) {
        val qianHaiDF = sc.textFile(hdfsPath).map(_.split("\t")).map { p =>
          if (p.length == 3) QianHai(p(0), p(1), p(2), "") else QianHai(p(0), p(1), p(2), p(3))
        }.toDF()
        Some(qianHaiDF)
      } else {
        LOG.error(s"目录 < $hdfsPath > 不存在")
        None
      }
    }
    else {
      LOG.info(s"参数 <$hdfsPath> 为空或不存在")
      None
    }
  }


  def dataFrameIsEmptyOrElse(dataFrame: DataFrame*): Boolean = {
    var k = true
    for (i <- dataFrame if i.head() == null) {
      k = false
    }
    k
  }

  def readParquet(ctx: HiveContext, centerResultPath: String): Option[DataFrame] = {
    if (centerResultPath != "" || centerResultPath != null) {

      if (hdfsUtils.dirExists(centerResultPath)) {
        val df = Some(ctx.read.parquet(centerResultPath))
        df
      } else {
        LOG.error(s"hdfs  目录 < $centerResultPath > 不存在")
        None
      }
    }
    else {
      LOG.info(s"参数 <$centerResultPath> 为空或不存在")
      None
    }
  }


  def mergeParquet(ctx: HiveContext,
                   centerFilePath: String,
                   savePath: String,
                   sqlMap: mutable.LinkedHashMap[
                     String,
                     Array[(String, String, Seq[String])]]
                  ) {

    val list = ListBuffer.empty[(DataFrame, Seq[String], String)]
    list.append(null)

    LOG.info(s"开始执行mergeParquet任务")

    sqlMap.foreach(x => {
      x._2.foreach(y => {
        if (y._1 != "incre_loan_150_info" && y._1 != "loan_150_info") {
          list.append((readParquet(ctx, s"$centerFilePath/${y._1}").get, y._3, y._1))
          LOG.info(s"读取<${centerFilePath + y._1}> 文件,成功添加至listBuffer中")
        }
      })
    })

    //    list.append((dataFrame, Seq("loan_id", "channel")))
    for (i <- 1 until list.size) {
      println(s"df标记 ${list(i)._3}  ----${list(i)._1.show(1)}")
    }

    for (i <- 1 until list.size - 1) {
      if (i == 1) {
        list(0) = (list(i)._1.join(list(i + 1)._1, list(i + 1)._2, "full"), Seq(""), "")
        list(1)._1.unpersist()
      }
      else {
        list(i)._1.unpersist()
        list(0) = (list.head._1.join(list(i + 1)._1, list(i + 1)._2, "full"), Seq(""), "")
      }
      list.last._1.unpersist()
      LOG.info(s"注意这里：join结果大小 <${list.head._1.count()}>")
    }

    dataFrameWriteToParquet(appendOrOverwrite = false, list.head._1, savePath)

  }

  /**
    * 当前渠道回溯的过去1个月平均授信通过率E
    *
    * @param sc       SparkContext
    * @param hdfsPath 通过率文件的路径,
    *                 文件格式为：授信审核通过笔数,授信申请已审核笔数,一级渠道,统计日期（‘\t’分割）
    * @return (channel_day,pass_rate)
    */
  def pass30AvgRate(sc: SparkContext, hdfsPath: String): RDD[(String, String)] = {
    sc.textFile(hdfsPath).map(line => {
      val lines = line.split("\t")
      val passNum = lines(0)
      val applyNum = lines(1)
      val channel = lines(2)
      val time = lines(3)
      (channel, (passNum, applyNum, time))
    }).groupByKey().map(x => (x._1, x._2.toList.sortBy(_._3)))
      .map(x => (x._1, compRateByChannel(x._2)))
      .flatMap(x => x._2.map(y => (x._1 + "_" + y._1, y._2)))
  }

  /**
    * 用来求每天向前推30天的平台通过率
    *
    * @param alist 渠道对应的list
    * @return 渠道HashMap(天,此天及30天的通过率)
    */
  def compRateByChannel(alist: List[(String, String, String)]): mutable.HashMap[String, String] = {
    val dayAndRate = mutable.HashMap[String, String]()
    for (ones <- alist.indices.reverse) {
      var passNum = 0
      var applyNum = 0
      val day = alist(ones)._3
      if (ones - 28 > 0) {
        val begin = ones - 29
        for (wt <- begin.to(ones)) {
          val oneTup = alist(wt)
          passNum += oneTup._1.toInt
          applyNum += oneTup._2.toInt
        }
        dayAndRate.put(day, (passNum / applyNum.toDouble).formatted("%.4f"))
      } else {
        for (wt <- 0.to(ones)) {
          val oneTup = alist(wt)
          passNum += oneTup._1.toInt
          applyNum += oneTup._2.toInt
        }
        dayAndRate.put(day, (passNum / applyNum.toDouble).formatted("%.4f"))
      }
    }
    dayAndRate
  }
}
