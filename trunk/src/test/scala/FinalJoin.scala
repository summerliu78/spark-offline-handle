import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/10/26.
  */
object FinalJoin {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("CallsHandle")
      .master("local[*]")
      .getOrCreate()


    val sc = spark.sparkContext


    val retur = sc.textFile("C:\\Users\\think\\Downloads\\res.csv").map(x => {
      val s = x.split(",")
      (s(0).replaceAll("^[0-9]+.", ""), (s(0), s.mkString(",")))
    })

    val over = sc.textFile("C:\\Users\\think\\Downloads\\overDue.txt").map(x => {
      val s = x.split(",")
      val a = s(0)
      val b = s(1)
      (a, b)
    })

    //retur.take(100).foreach(println)
    //over.take(100).foreach(println)
    //    retur.join(over).take(100).foreach(println)

    //    println("1258737_7972070_weibo".replaceAll("^[0-9]+.",""))


    //
    var num = 0
    val v = retur.join(over).map(x => {
      s"${x._1},${x._2._1._2},${x._2._2}"
    })
      //      .map(x => {
      //      if (x._2._2.isEmpty) {
      //        s"${x._1},${x._2._1},"
      //      } else {
      //        num += 1
      //        s"${x._1},${x._2._1},${x._2._2.get}"
      //      }
      //    })
      .coalesce(1).saveAsTextFile("E:/lwTest/tttttttttt")
    println(s"size : $num")

  }
}
