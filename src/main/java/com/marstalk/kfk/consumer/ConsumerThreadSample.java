package com.marstalk.kfk.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 每个线程对应一个KafkaConsumer实例，解决线程安全问题。 业务性数据，在失败的时候可以rollback。数据不能丢失，类似TCP 一般来说一个分区对应一个Consumer
 */
public class ConsumerThreadSample {

    public static void main(String[] args) {
        //例子中，有topic_ljc有三个分区，所以这里使用三个线程来处理，每个线程有一个consumer
    }

    static class Workder implements Runnable {

        //泛型了消息的key和value。本例子中都是String。
        private KafkaConsumer<String, String> consumer;
        //TODO 为什么需要volatile修饰呢？
        private volatile boolean shutdown;

        public Workder(String topic, int partitionId) {
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("aliyun_beidou_kafka"));
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.assign(Arrays.asList(new TopicPartition(topic, partitionId)));
            this.consumer = consumer;
        }

        @Override
        public void run() {
            while (!shutdown) {

            }

        }

        public void shutdown() {
            this.shutdown = true;
            //唤醒consumer去shutdown。//TODO
            consumer.wakeup();
        }

    }

}
