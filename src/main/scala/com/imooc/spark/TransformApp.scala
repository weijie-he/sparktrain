package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformApp {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    //    创建StreamingContext需要2个参数：SparkConf 和 batch interval
    val ssc = new StreamingContext(sc,Seconds(5))

//    构建黑名单
    val blackList = List("zs","ls")
//      要先获取sparkContext
    val blackRdd = ssc.sparkContext.parallelize(blackList).map(x=>(x,true))

    val lines = ssc.socketTextStream("192.168.1.101",6789)
//    lines map完后仍是个Dstream，而blackRdd是个RDD。需要用transform转化
    val clicklog = lines.map(x=>(x.split(",")(1),x)).transform(rdd=>{
      rdd.leftOuterJoin(blackRdd).
        filter(_._2._2.getOrElse(false)!=true).
        map(_._2._1)
    })
    clicklog.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
