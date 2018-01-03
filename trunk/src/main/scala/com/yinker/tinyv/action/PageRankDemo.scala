package com.yinker.tinyv.action

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by think on 2017/11/13.
  */
object PageRankDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .appName("CallsHandle")
      .master("local[*]")
      .config("hive.exec.parallel", "true")
      .config("hive.exec.compress.output", "true")
      .config("mapred.output.compress", "true")
      .config("hive.exec.compress.intermediate", "true")
      .config("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("hive.intermediate.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
      .config("hive.auto.convert.join", "false")
      .config("spark.some.config.option", "some-value")
      .config("spark.sql.hive.convertMetastoreOrc", "false")
      .config("hive.mapjoin.localtask.max.memory.usage", "0.99")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()



    val readDir=args(0)
    val saveDir=args(1)

//args 1  save dir  args 0  read dir
    DoPageRank(spark.sparkContext, saveDir,GetDataFromLocal(readDir, spark.sparkContext))

    //    val path = "E:/project/data/combined.txt"
    //    val egeRDD = spark.sparkContext.textFile(path).map { x =>
    //      val fields = x.split(",")
    //      Edge(fields(0).toLong, fields(1).toLong, None)
    //    }
    //    val graph = Graph.fromEdges(egeRDD, 0)
    //    val pageRank = graph.pageRank(0.0001).cache()
    //    pageRank.vertices.foreach(println)
  }


  def GetDataFromLocal(localDir: String, sc: SparkContext): RDD[(String, String)] = {
    sc.textFile(localDir).map(x => {
      val s = x.split("\t")
      if (s(2) == "012001001") (s(0), s(1)) else (s(1), s(0))
    }).filter(x => {
      x._1.length == 11 && x._1.head == '1' && x._2.length == 11 && x._2.head == '1'
    }).distinct()
  }


  def DoPageRank(sc: SparkContext, saveDir: String, rdd: RDD[(String, String)]) {
    val egeRDD = rdd.map { x =>
      Edge(x._1.toLong, x._2.toLong, None)
    }
    val graph = Graph.fromEdges(egeRDD, 0)
    val pageRank = graph.pageRank(0.0001).cache()
    pageRank.vertices.saveAsTextFile(saveDir)
  }


}
