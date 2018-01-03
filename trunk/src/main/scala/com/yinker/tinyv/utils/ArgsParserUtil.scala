package com.yinker.tinyv.utils

import java.util.Date

/**
  * Created by think on 2017/8/15.
  */
class ArgsParserUtil(args: Array[String]) {

  private var sysProperties = Map.empty[String, String]
  args.foreach { x =>
    val array = x.split("=")
    if (array.length == 2) {
      val key = array(0).trim()
      val value = array(1).trim()
      sysProperties += ((key, value))
    }
  }

  def getPropertyOrElse(propName: String, defaultValue: String, print: Boolean): String = {
    val prop = sysProperties.getOrElse(propName, defaultValue)
    if (print) println(s"${new Date}: $propName=$prop")
    prop
  }


}

case class ModelArgs(args: Array[String]) extends Serializable {

  def argsParse: ArgsParserUtil = new ArgsParserUtil(args)

  /**
    * task.type represent one of [AutoFirstProcessor, AutoSecondProcessor, AutoThirdProcessor]
    *
    * @return
    */
  def jobType: String = argsParse.getPropertyOrElse("task.type", "write_parquet", print = true)

  /**
    * task.scale represent one of [incre, full]
    * incre: 跑增量数据
    * full: 跑全量数据
    *
    * @return
    */
  def jobScale: String = argsParse.getPropertyOrElse("task.scale", "incre", print = true)

  /**
    * task.scheduler represent one of [rely,merge,union,join]
    * rely：基于每个sql的processor跑的数据(包含全量和增量)
    * merge：将rely跑出的数据按照条件进行join，得到不包括dueday的大表
    * union：将增量数据的merge结果与全量的merge结果进行union，得到包括增量但不包括dueday的大表
    * join：将union的结果与dueday的结果进行join得到最后包含所有字段的大表
    *
    * @return
    */
  def jobScheduler: String = argsParse.getPropertyOrElse("task.scheduler", "", print = true)


}

case class CallslArgs(args: Array[String]) extends Serializable {


  def argsParse: ArgsParserUtil = new ArgsParserUtil(args)


  /**
    *
    * @return job type seven type of [ stableContract3mthCountPath.......... ]
    */
  def jobScale: String = argsParse.getPropertyOrElse("task.scale", "", print = true)
}

case class PathArgs() extends Serializable {

  val modelRootPath = "/xiaov/xiaov_CallDetails/test/model_data"

  def sqlFileLocalPath = s"/xiaov/xiaov_CallDetails/model_data/sql"

  def centerFilePath = s"$modelRootPath/center_data/${DateUtils.getNowDate("yyyy-MM-dd")}"

  //  def centerFilePath = s"$modelRootPath/center_data/2017-08-18/"

  def increCenterFilePath = s"$modelRootPath/incre_center_data/${DateUtils.getNowDate("yyyy-MM-dd")}"

  // 前海txt文件的路径
  def qianHaiFilePath = s"$centerFilePath/qianhai/*"

  def increQianHaiFilePath = s"$increCenterFilePath/qianhai/*"

  //全量数据merge结果
  def fullMergeDataPath = s"$modelRootPath/merge_data/full_data/${DateUtils.getNowDate("yyyy-MM-dd")}"

  def fullMergeDataCopyPath = s"$modelRootPath/merge_data/full_data/${DateUtils.getNowDate("yyyy-MM-dd")}_copy"

  def beforeFullMergeDataPath = s"$modelRootPath/merge_data/full_data/${DateUtils.getBeforeDay("yyyy-MM-dd", 1)}"

  def beforeFullMergeDataCopyPath = s"$modelRootPath/merge_data/full_data/${DateUtils.getBeforeDay("yyyy-MM-dd", 1)}"

  def increDataMergePath = s"$modelRootPath/merge_data/incre_data/${DateUtils.getNowDate("yyyy-MM-dd")}"

  def fullIncreDataMergePath = s"$modelRootPath/merge_data/full_incre_data/${DateUtils.getNowDate("yyyy-MM-dd")}"

  def dueDayDataPath = s"$modelRootPath/dueday_data/${DateUtils.getNowDate("yyyy-MM-dd")}"

  def finalDataPath = s"$modelRootPath/final_data/${DateUtils.getNowDate("yyyy-MM-dd")}"

}


case class QianHai(loan_id: String, channel: String, qh_long_position_score: String, qh_institution_num: String)