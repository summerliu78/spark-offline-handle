package com.yinker.tinyv.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer

/**
  * Created by ThinkPad on 2017/6/9.
  */
class HdfsUtils extends Serializable {
  val LOG: Logger = Logger.getLogger(this.getClass)
  private val hadoopConf = new Configuration()
  private val fs = FileSystem.get(hadoopConf)


  /**
    *
    * @param hdfsDirectory 传入的多个hdfs文件目录
    * @return 有一个文件目录不存在 返回false
    */
  def dirExists(hdfsDirectory: String*): Boolean = {
    var k = true
    for (i <- hdfsDirectory if !fs.exists(new Path(i))) {
      k = false
    }
    k
  }


  def dfsStatus(path: String): ListBuffer[(String, String)] = {
    val list = ListBuffer.empty[(String, String)]

    list
  }

  /**
    *
    * @param path 需要创建的路径
    */
  def mkdirOnHDFS(path: String): Unit = {
    if (!dirExists(path)) {
      LOG.info(s"目录$path 不存在")
      fs.mkdirs(new Path(path))
      LOG.info(s"目录$path 创建成功")
    }
  }

  /**
    *
    * @param src 需要拷贝的目录
    * @param dst 拷贝到的目录
    */
  def copyFile(src: String, dst: String) {

    //1:建立输入流
    val input = fs.open(new Path(src))

    //2:建立输出流
    val output = fs.create(new Path(dst))

    if (pathExistDelete(dst)) IOUtils.copyBytes(input, output, 4096, true)

  }


  /**
    *
    * @param path 删除的路径
    * @return 删除后需要删除的目录不存在返回ture
    */
  def pathExistDelete(path: String): Boolean = {
    if (dirExists(path)) {
      fs.delete(new Path(path), true)
      LOG.info(s"删除$path 成功")
    }
    !dirExists(path)
  }


  /**
    *
    * @param path 传入的需要判断状态的路径
    * @return 文件不为空且 大小大于0 返回true  否则返回false
    */

  def getFileStatus(path: String): Boolean = {
    if (dirExists(path)) {
      //      val status = fs.getFileStatus(new Path(path))
      //      val fileLastChangeTime = status.getModificationTime
      //      val fileSize = status.getBlockSize
      val fileSize = fs.getContentSummary(new Path(path)).getSpaceConsumed
      //      status.getAccessTime
      LOG.info(s"文件 <$path> 大小为: $fileSize")
      fileSize > 0
    } else {
      LOG.error(s"目录 $path 不存在")
      false
    }
  }


  /**
    *
    * @param path 传入的路径
    * @return 返回此目录下所有的子目录及文件 返回一个list
    */

  def getDirList(path: String): ListBuffer[String] = {
    val list: ListBuffer[String] = ListBuffer.empty
    val listStatus = fs.listStatus(new Path(path))
    for (i <- 0 until listStatus.length) {
      listStatus(i).getPath.getName :+ list
    }
    list
  }


}