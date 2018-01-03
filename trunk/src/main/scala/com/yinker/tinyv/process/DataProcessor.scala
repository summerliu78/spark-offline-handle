package com.yinker.tinyv.process

import com.yinker.tinyv.utils.{DataUtils, HdfsUtils, PathArgs}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable

/**
  * Created by spark team on 2017/8/17.
  */
object DataProcessor {
  val LOG: Logger = Logger.getLogger(this.getClass)
  val pathArgs = PathArgs()
  val hdfsUtils = new HdfsUtils()


  /**
    * 全量任务
    *
    * @param sc      SparkContext
    * @param ctx     HiveContext
    * @param jobType 运行任务的层级顺序
    */
  def runRely(sc: SparkContext,
              ctx: HiveContext,
              jobScale: String,
              jobType: String
             ): Unit = {

    //必须是这三个任务中的一个 防止submit脚本中参数漏写或写错


    println(s"jobtype: <$jobType>  ")

    println(s"jobScale: <$jobScale>")

    //不管全量增量  都只需要跑一次 dueDay mark的任务
    if (jobType == "AutoSecondProcessor") {

      if (hdfsUtils.pathExistDelete(pathArgs.dueDayDataPath)) {
        DataUtils.writeParquet(
          sc,
          ctx,
          s"${pathArgs.sqlFileLocalPath}/${dueDayMap("due_mask_info")._1}",
          pathArgs.dueDayDataPath)
      }
    }


    //判断是跑第几个任务
    if (jobType == "AutoFirstProcessor" || jobType == "AutoSecondProcessor" || jobType == "AutoThirdProcessor") {

      //判断是全量还是增量
      if (jobScale == "incre") {
        LOG.info(s"job scale $jobScale run..")
        if (increTaskMap.contains(jobType)) {
          increTaskMap(jobType).foreach(tup => {
            //读出其中的sql任务 并执行sql 后 落地
            DataUtils.writeParquet(sc, ctx, s"${pathArgs.sqlFileLocalPath}/${tup._2}", s"${pathArgs.increCenterFilePath}/${tup._1}")
          })
        } else {
          LOG.error(s"taskMap(jobType) k   $jobType 不存在")
        }
      }
      else if (jobScale == "full") {
        LOG.info(s"job scale $jobScale run..")
        if (taskMap.contains(jobType)) {
          taskMap(jobType).foreach(tup => {
            //读出其中的sql任务 并执行sql 后 落地
            DataUtils.writeParquet(sc, ctx, s"${pathArgs.sqlFileLocalPath}/${tup._2}", s"${pathArgs.centerFilePath}/${tup._1}")
          })
        } else {
          LOG.error(s"taskMap(jobType) k   $jobType 不存在")
        }
      }
      else {
        LOG.error(s"job Scale < $jobScale > not exsit!!")
        System.exit(1)
      }
    }

    else {
      LOG.error(s"jobType $jobType not exsit," +
        s"please check it (type in [AutoFirstProcessor|AutoSecondProcessor|AutoThirdProcessor])")
      System.exit(1)
    }
  }


  //"loan_id","channel"


  def runMerge(sc: SparkContext,
               ctx: HiveContext,
               jobScale: String
              ): Unit = {


    if (jobScale == "full") {
      val resTaskMap = taskMap.map(x => {
        if (x._1 == "AutoThirdProcessor") x._2.drop(0)
        x
      })

      DataUtils.mergeParquet(
        ctx,
        pathArgs.centerFilePath,
        pathArgs.fullMergeDataPath,
        resTaskMap)

      //      hdfsUtils.copyFile(s"${pathArgs.fullMergeDataPath}/*", pathArgs.fullMergeDataCopyPath)

    } else if (jobScale == "incre") {
      val resIncreTaskMap = increTaskMap.map(x => {
        if (x._1 == "AutoThirdProcessor") x._2.drop(0)
        x
      })
      DataUtils.mergeParquet(
        ctx,
        pathArgs.increCenterFilePath,
        pathArgs.increDataMergePath,
        resIncreTaskMap)
    }
    else {
      LOG.error(s"jobScale $jobScale not exsit")
      sys.exit(1)
    }
    LOG.info(s"mergeParquet 任务执行成功 任务类型 <$jobScale>")
  }

  def runUnionIncre(ctx: HiveContext) {

    //昨天的全量数据merge结果的copy和今天的增量数据merge结果同时存在union
    if (hdfsUtils.dirExists(pathArgs.beforeFullMergeDataPath, pathArgs.increDataMergePath)) {
      val beforeFullMergeDataDF = DataUtils.readParquet(ctx, pathArgs.beforeFullMergeDataPath).get
      val increDataMergeDF = DataUtils.readParquet(ctx, pathArgs.increDataMergePath).get
      if (DataUtils.dataFrameIsEmptyOrElse(beforeFullMergeDataDF, increDataMergeDF)) {
        DataUtils.dataFrameWriteToParquet(
          appendOrOverwrite = false,
          beforeFullMergeDataDF.unionAll(increDataMergeDF),
          pathArgs.fullIncreDataMergePath)

        //卸载
        beforeFullMergeDataDF.unpersist()
        increDataMergeDF.unpersist()
      } else {
        LOG.error(s"数据集beforeFullMergeDataDF increDataMergeDF 至少有一个为没有数据，size为0")
        sys.exit(1)
      }
    }
  }


  def runFinalJoin(ctx: HiveContext, jobScale: String): Unit = {

    val dueDayDataDF = DataUtils.readParquet(ctx, pathArgs.dueDayDataPath).get


    if (jobScale == "full") {
      val fullMergeDataDF = DataUtils.readParquet(ctx, pathArgs.fullMergeDataPath).get
      //判断两个的size都不为0
      if (DataUtils.dataFrameIsEmptyOrElse(dueDayDataDF, fullMergeDataDF)) {
        DataUtils.dataFrameWriteToJson(
          appendOrOverwrite = false,
          fullMergeDataDF.join(dueDayDataDF, dueDayMap("due_mask_info")._2),
          pathArgs.finalDataPath)
      } else {
        LOG.error("full 任务的 fullDataMergeDF 或者 dueDayDataDF 至少有一个DF为空")
      }
    } else if (jobScale == "incre") {
      val beforeFullDataMergeData = DataUtils.readParquet(ctx, pathArgs.beforeFullMergeDataPath).get
      if (DataUtils.dataFrameIsEmptyOrElse(beforeFullDataMergeData, dueDayDataDF)) {
        DataUtils.dataFrameWriteToJson(
          appendOrOverwrite = false,
          beforeFullDataMergeData.join(dueDayDataDF, dueDayMap("due_mask_info")._2),
          pathArgs.finalDataPath)
      }
    }
  }

  /**
    *
    * @return 返回有序map 任务次序 -> (存的是保存结果名,sql文件名,该sql的join依赖字段)
    */
  def taskMap = mutable.LinkedHashMap(
    "AutoFirstProcessor" -> Array(
      ("user_info_new", "user_info_new.sql", Seq("loan_id", "channel")),
      ("loan_miguan_info", "loan_miguan_info.sql", Seq("loan_id", "channel")),
      ("loan_qianhai_info", "loan_qianhai_info.sql", Seq("loan_id", "channel")),
      ("loan_weibo_info", "loan_weibo_info.sql", Seq("loan_id", "channel")),
      ("network_loan_info", "network_loan_info.sql", Seq("loan_id", "channel"))
    ),
    "AutoSecondProcessor" -> Array(
      ("loan_td_info", "loan_td_info.sql", Seq("loan_id", "channel")),
      ("td_risk_credit_info", "td_risk_credit_info.sql", Seq("loan_id", "channel"))
    ),
    "AutoThirdProcessor" -> Array(
      ("loan_150_info", "loan_150_info.sql", Seq("mobile", "channel")),
      ("call_detail_info", "call_detail_info.sql", Seq("mobile", "channel")),
      ("emergency_update_info", "emergency_update_info.sql", Seq("mobile", "channel")),
      ("150_in3_info", "150_in3_info.sql", Seq("mobile", "channel"))
    )


  )

  //incre_
  def increTaskMap = mutable.LinkedHashMap(
    "AutoFirstProcessor" -> Array(
      ("incre_user_info_new", "incre_user_info_new.sql", Seq("loan_id", "channel")),
      ("incre_loan_miguan_info", "incre_loan_miguan_info.sql", Seq("loan_id", "channel")),
      ("incre_loan_qianhai_info", "incre_loan_qianhai_info.sql", Seq("loan_id", "channel")),
      ("incre_loan_weibo_info", "incre_loan_weibo_info.sql", Seq("loan_id", "channel")),
      ("incre_network_loan_info", "incre_network_loan_info.sql", Seq("loan_id", "channel"))
    ),
    "AutoSecondProcessor" -> Array(
      ("incre_loan_td_info", "incre_loan_td_info.sql", Seq("loan_id", "channel")),
      ("incre_td_risk_credit_info", "incre_td_risk_credit_info.sql", Seq("loan_id", "channel"))
    ),
    "AutoThirdProcessor" -> Array(
      ("incre_loan_150_info", "incre_loan_150_info.sql", Seq("mobile", "channel")),
      ("incre_call_detail_info", "incre_call_detail_info.sql", Seq("mobile", "channel")),
      ("incre_emergency_update_info", "incre_emergency_update_info.sql", Seq("mobile", "channel")),
      ("incre_150_in3_info", "incre_150_in3_info.sql", Seq("mobile", "channel"))
    )
  )

  def dueDayMap = Map("due_mask_info" -> ("due_mask_info.sql", Seq("loan_id", "channel")))

}
