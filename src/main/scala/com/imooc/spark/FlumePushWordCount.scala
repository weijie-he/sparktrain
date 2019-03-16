package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark Streaming 整合Flume （push方式）
  */
object FlumePushWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length!=2){
      System.err.println("参数不对")
      System.exit(1)
    }

    val Array(hostname,port) = args

//    生产需要环境把setMaster等注释掉，而用submit提交
    val sparkConf = new SparkConf()//.setMaster("local[3]").setAppName("FlumePushWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

//    Note: Flume support is deprecated as of Spark 2.3.0.
    val flumeStream = FlumeUtils.createStream(ssc,hostname,port.toInt)

//    flume在传输的时候是有head，有body的。需要先得到body.再转为字节数组
    flumeStream.map(x=>new String(x.event.getBody.array()).trim)
        .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
