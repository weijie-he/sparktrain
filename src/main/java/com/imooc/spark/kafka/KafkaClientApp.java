package com.imooc.spark.kafka;

public class KafkaClientApp {
    public static void main(String[] args) {
        new MyKafkaProducer(KafkaProperties.TOPIC).start();
    }
}
