package com.yinker.tinyv.utils

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by Reynold on 17-8-28.
  */
object InkConfig {
  private val conf: Config = ConfigFactory.load()
  val MYSQL_LOGIC_URL_PT: String = conf.getString("mysql_pt.logic.url")
  val MYSQL_LOGIC_URL_WB: String = conf.getString("mysql_wb.logic.url")
  // 业务研发字段计算
  val NEAR3MONTHPAALYHIT: String = conf.getString("near3MonthApplyHit")
  val NEAR1MONTHSEARCH: String = conf.getString("near1MonthSearch")
  val HISTORYSEARCH: String = conf.getString("historySearch")
  val EMERCONTACTCOUNT: String = conf.getString("emerContactCount")

  val MYSQL_LOGIC_PROPERTIES_PT: Properties = propsFromConfig(conf.getConfig("mysql_pt.logic.properties"))
  val MYSQL_LOGIC_PROPERTIES_WB: Properties = propsFromConfig(conf.getConfig("mysql_wb.logic.properties"))

  private def propsFromConfig(config: Config): Properties = {
    import scala.collection.JavaConversions._

    val props = new Properties()

    val map: Map[String, Object] = config.entrySet().map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)

    props.putAll(map)
    props
  }

  def main(args: Array[String]): Unit = {
    println(conf.getConfig("mysql_pt.logic.properties").entrySet())
    println(MYSQL_LOGIC_URL_PT)
    println(MYSQL_LOGIC_PROPERTIES_WB)
  }
}
