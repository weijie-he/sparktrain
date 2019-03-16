package com.imooc.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用Spark Streaming 处理文件系统的数据
  */
object FileWordCount {
  def main(args: Array[String]): Unit = {
//    读取本地文件可以不用多线程
    val sc = new SparkConf().setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(sc,Seconds(5))

    val lines = ssc.textFileStream("file://C:\\Users\\hwj\\Videos\\Wreck-It.Ralph.2012.720p.BluRay.x264-SPARKS [PublicHD]")
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
