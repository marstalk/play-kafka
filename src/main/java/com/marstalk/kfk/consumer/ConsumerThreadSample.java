package com.marstalk.kfk.consumer;

import com.marstalk.kfk.admin.AdminSample;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 每个线程对应一个KafkaConsumer实例，解决线程安全问题。 业务性数据，在失败的时候可以rollback。数据不能丢失，类似TCP 一般来说一个分区对应一个Consumer
 */
public class ConsumerThreadSample {

    public static void main(String[] args) {
        AtomicInteger ai = new AtomicInteger();
        //例子中，有topic_ljc有三个分区，所以这里使用三个线程来处理，每个线程有一个consumer
        ExecutorService executorService = new ThreadPoolExecutor(5,
                5,
                1000,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10), r -> new Thread(r, "KafkaConsumer" + ai.getAndIncrement()));
        for (int i = 0; i < 3; i++) {
            executorService.submit(new Workder(AdminSample.TOPIC_LJC, i));
        }
    }

    static class Workder implements Runnable {

        //泛型了消息的key和value。本例子中都是String。
        private String workerName;
        private KafkaConsumer<String, String> consumer;
        private TopicPartition topicPartition;
        //TODO 为什么需要volatile修饰呢？
        private volatile boolean shutdown;

        public Workder(String topic, int partitionId) {
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("aliyun_beidou_kafka"));
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            this.topicPartition = new TopicPartition(topic, partitionId);
            this.consumer = new KafkaConsumer<>(props);
            this.consumer.assign(Arrays.asList(topicPartition));
            this.workerName = "worker" + partitionId;
        }

        @Override
        public void run() {
            while (!shutdown) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
                List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
                records.forEach(r -> System.out.printf("worker=%s, partition=%d, topic=%s, key=%s, value=%s", workerName, r.partition(), r.topic(), r.key(), r.value()));
            }
            //【一定要记得关闭】
            consumer.close();
            System.out.println(workerName + " Consumer shutdown successfully " + consumer);
        }

        /**
         * shutdown hook
         */
        public void shutdown() {
            this.shutdown = true;
            //唤醒consumer去shutdown。//TODO wakeup具体是做什么？线程唤醒嘛？
            consumer.wakeup();
        }

    }

}
