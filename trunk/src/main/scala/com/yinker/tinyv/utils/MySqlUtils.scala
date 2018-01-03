package com.yinker.tinyv.utils

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.yinker.tinyv.process.SauronProcessor
import org.apache.ibatis.ognl.Ognl
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
  * Created by Reynold on 17-8-28.
  */
object MySqlUtils {

  def loadMySQL(url: String,
                table: String,
                spark: SparkSession,
                connProp: Properties): DataFrame = {

    spark.read.jdbc(url, table, connProp)
  }

  def saveMySQL(url: String,
                df: DataFrame,
                table: String,
                sqlContext: SQLContext,
                connProp: Properties): Unit = {
    df.write.mode(SaveMode.Append).jdbc(url, table, connProp)
  }


}
