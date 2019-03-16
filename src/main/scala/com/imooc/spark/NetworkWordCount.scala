package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark Streaming 处理 Socket 数据
  */
object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
//    创建StreamingContext需要2个参数：SparkConf 和 batch interval
    val ssc = new StreamingContext(sc,Seconds(5))

    val lines = ssc.socketTextStream("192.168.1.101",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
