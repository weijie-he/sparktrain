# Spark Streaming整合Flume&Kafka打造通用流处理平台

### 一、概述
<img src="https://ws1.sinaimg.cn/large/50c1811fgy1g14vfnhuixj20vs0lajwl.jpg" width="60%" height="300px" >

<p align="center">流程示意图</p>

先通过log4j模拟日志的产生，再用Flume收集日志，Flume再通过Kafka Sink 对接到Kafka，Kafka再对接到Spark Streaming 进行实时流处理，然后可以将结果入库或进行UI展示。

### 二、具体实现

#### 2.1 写一个LoggerGenerator 模拟日志产生

```java
public class LoggerGenerator {
    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws InterruptedException {
        int index =0;
        while (true){
            Thread.sleep(2000);
            logger.info("current value is "+index++);
        }
    }
}
```

可以配置一下 log4j.properties 文件，让日志按自己喜欢的格式输出。

#### 2.2 将LoggerGenerator 与Flume对接

这部可以通过Log4J Appender实现，参考官网：http://flume.apache.org/releases/content/1.6.0/FlumeUserGuide.html#log4j-appender

需要注意以下几点：

1. flume 的agent要用 avro source
2. 需要添加Maven依赖（flume-ng-sdk-1.6.0.jar）
3. log4j.properties里需要写点东西

```java
#...
log4j.appender.flume = org.apache.flume.clients.log4jappender.Log4jAppender
log4j.appender.flume.Hostname = example.com
log4j.appender.flume.Port = 41414
log4j.appender.flume.UnsafeMode = true

# configure a class's logger to output to the flume appender
log4j.logger.org.example.MyClass = DEBUG,flume
#...
```

#### 2.3 将Flume与Kafka对接

先修改Flume的配置文件

```java
agent1.sources = avro-source
agent1.sinks = kafka-sink
agent1.channels = logger-channel

agent1.sources.avro-source.type = avro
agent1.sources.avro-source.bind = 192.168.1.101
agent1.sources.avro-source.port = 12345

agent1.sinks.kafka-sink.type = org.apache.flume.sink.kafka.KafkaSink
agent1.sinks.kafka-sink.topic = kafka_streaming
agent1.sinks.kafka-sink.brokerList =localhost:9092
agent1.sinks.kafka-sink.requiredAcks = 1
#设置batchSize为5则表示来了5条记录，消费者那边才会显示1次
agent1.sinks.kafka-sink.batchSize = 5

agent1.channels.logger-channel.type = memory
agent1.channels.logger-channel.capacity = 1000
agent1.channels.logger-channel.transactionCapacity = 100

agent1.sources.avro-source.channels = logger-channel
agent1.sinks.kafka-sink.channel = logger-channel
```

然后可以启动Flume、Kafka消费者，再跑一下之前写的LoggerGenerator，可以发现 Kafka消费者这边有数据了。

#### 2.4 将Kafka与Spark Streaming对接

不同的Kafka版本有不同的连接方式，参考官网：http://spark.apache.org/docs/latest/streaming-kafka-integration.html

我用的是Kafka1.1.0，编写了以下程序

```scala
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

```

#### 2.6 将计算结果写入MySQL中

参考官网：http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd

有两种推荐的方式

1. 对于每个Partition创建一个连接
2. 使用连接池

此处我采用了第一种方式，可参考以下代码

```scala
result.foreachRDD(rdd => {
      //      对每个分区创建一个连接
      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        //          再对分区中的每条记录操作，还要写一次循环
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"
          connection.createStatement().execute(sql)
        })

        connection.close()
      })
    })
```

```scala
 /**
    * 获取MySQL的连接
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test?charset=utf8&useSSL=false", "root", "123456")
  }
```

