package com.marstalk.kfk.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerSample {

    public static void main(String[] args) {

    }

    private static void helloWorld(){
        Properties props = new Properties();
        
        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");
        props.setProperty("bootstrap.servers", aliyun_beidou_kafka);
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));


    }
}
