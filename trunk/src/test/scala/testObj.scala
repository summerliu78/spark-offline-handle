import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/10/25.
  */
object testObj {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("CallsHandle")
      .master("local[*]")
      .getOrCreate()
    //
    //    val sql =
    //      s"""
    //         |select
    //         |        biz_id loan_id
    //         |        ,user_id
    //         |        ,ds channel
    //         |        ,resultdata res
    //         |from
    //         |        bdw_tinyv_outer.b_fuse_handle_result_mongo_i
    //         |where
    //         |        dt <= '2017-10-23' and dt >= '2017-09-05' and biz_type='credit'
    //       """.stripMargin
    val sc = spark.sparkContext
    //    spark.sql(sql).rdd.map(x => {
    //      val loan_id = x.getAs[String]("loan_id")
    //      val user_id = x.getAs[String]("user_id")
    //      val channel = x.getAs[String]("channel")
    //      val res = x.getMap[String, String](3)
    //      (s"${loan_id}_${user_id}_$channel", res)
    //    }).groupByKey()


    //    val callsRDD = sc.textFile("C:/Users/think/Downloads/TXL.txt").distinct()
    val info = sc.textFile("C:/Users/think/Downloads/rd.txt")

    val a = info.filter(_.split(",").length == 14).map(x => {
      val s = x.split(",")
      val k = s"${s(0)}_${s(1)}_${s(2)}"
      (k, (s(3), s(4), s(5), s(6), s(7), s(8), s(9), s(10), s(11), s(12), s(13)))
    })
      .combineByKey(
        List(_),
        (x: List[(String, String, String, String, String, String, String, String, String, String, String)],
         y: (String, String, String, String, String, String, String, String, String, String, String)) => y +: x,
        (x: List[(String, String, String, String, String, String, String, String, String, String, String)],
         y: List[(String, String, String, String, String, String, String, String, String, String, String)]) => x ++ y)

      .map(x => {
        //        var xMap = new scala.collection.mutable.HashMap[String, String]()
        var a1 = ""
        var a2 = ""
        var a3 = ""
        var a4 = ""
        var a5 = ""
        var a6 = ""
        var a7 = ""
        var a8 = ""
        var a9 = ""
        var a10 = ""
        var a11 = ""
        x._2.map(y => {
          if (y._1.trim != "" && y._1.nonEmpty && y._1.trim != "NULL") a1 = y._1
          if (y._2.trim != "" && y._2.nonEmpty && y._2.trim != "NULL") a2 = y._2
          if (y._3.trim != "" && y._3.nonEmpty && y._3.trim != "NULL") a3 = y._3
          if (y._4.trim != "" && y._4.nonEmpty && y._4.trim != "NULL") a4 = y._4
          if (y._5.trim != "" && y._5.nonEmpty && y._5.trim != "NULL") a5 = y._5
          if (y._6.trim != "" && y._6.nonEmpty && y._6.trim != "NULL") a6 = y._6
          if (y._7.trim != "" && y._7.nonEmpty && y._7.trim != "NULL") a7 = y._7
          if (y._8.trim != "" && y._8.nonEmpty && y._8.trim != "NULL") a8 = y._8
          if (y._9.trim != "" && y._9.nonEmpty && y._9.trim != "NULL") a9 = y._9
          if (y._10.trim != "" && y._10.nonEmpty && y._10.trim != "NULL") a10 = y._10
          if (y._11.trim != "" && y._11.nonEmpty && y._11.trim != "NULL") a11 = y._11
          1
        })

        (x._1, s"$a1,$a2,$a3,$a4,$a5,$a6,$a7,$a8,$a9,$a10,$a11")
      })
      .coalesce(1).saveAsTextFile("E:/lwTest/rd")

  }
}
