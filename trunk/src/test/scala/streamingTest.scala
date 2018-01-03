//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.SparkSession
//
//import scala.util.parsing.json.JSON
//
///**
//  * Created by think on 2017/9/26.
//  */
//
//object streamingTest {
//  def main(args: Array[String]): Unit = {
//
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .config("hive.auto.convert.join", "false")
//      .config("spark.some.config.option", "some-value")
//      .config("spark.sql.hive.convertMetastoreOrc", "false")
//      .config("hive.mapjoin.localtask.max.memory.usage", "0.99")
//      .getOrCreate()
//
//    spark.sparkContext.textFile("C:\\Users\\think\\Desktop\\json_demo.txt")
//
//
//  }
//
//  def jsonCom(str: String): String = {
//    JSON.parseFull("") match {
//      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
//      case Some(map: Map[String, Any]) => println(map)
//      case None => println("Parsing failed")
//      case other => println("Unknown data structure: " + other)
//    }
//
//    null
//  }
//
//}
