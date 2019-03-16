package com.imooc.spark.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyKafkaProducer extends Thread{
    private String topic;

    private KafkaProducer producer;

    public MyKafkaProducer(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        properties.put("acks", "1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer(properties);

    }

    @Override
    public void run() {
        int messageNo=1;
        while (true){
            String message = "message_"+messageNo;
            try {
                producer.send(new ProducerRecord<String,String>(topic,message)).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            System.out.println("发送成功"+message);
            messageNo++;
            try {
                Thread.sleep(2000);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
