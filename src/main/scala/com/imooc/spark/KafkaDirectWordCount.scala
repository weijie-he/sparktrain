package com.imooc.spark
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object KafkaDirectWordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf，如果将任务提交到集群中，那么要去掉.setMaster("local[2]")

    val conf = new SparkConf().setAppName("DirectStream").setMaster("local[2]")

    //创建一个StreamingContext，其里面包含了一个SparkContext

    val streamingContext = new StreamingContext(conf, Seconds(5))



    //配置kafka的参数

    val kafkaParams = Map[String, Object](

      "bootstrap.servers" -> "192.168.1.101:9092",

      "key.deserializer" -> classOf[StringDeserializer],

      "value.deserializer" -> classOf[StringDeserializer],

      "group.id" -> "test123",

      "auto.offset.reset" -> "earliest", // lastest

      "enable.auto.commit" -> (false: java.lang.Boolean)

    )



    val topics = Array("kafka_streaming")

    //在Kafka中记录读取偏移量

    val stream = KafkaUtils.createDirectStream[String, String](

      streamingContext,

      //位置策略（可用的Executor上均匀分配分区）

      LocationStrategies.PreferConsistent,

      //消费策略（订阅固定的主题集合）

      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)

    )



    //迭代DStream中的RDD(KafkaRDD)，将每一个时间点对于的RDD拿出来

    stream.foreachRDD { rdd =>

      //获取该RDD对于的偏移量

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //拿出对应的数据

      rdd.foreach{ line =>

        println(line.key() + " " + line.value())

      }

      //异步更新偏移量到kafka中

      // some time later, after outputs have completed

      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }

    streamingContext.start()

    streamingContext.awaitTermination()

  }

}
