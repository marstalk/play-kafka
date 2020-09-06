package com.marstalk.kfk.producer;

import com.marstalk.kfk.admin.AdminSample;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Producer是线程安全的，所以建议多个线程共用一个producer实例。
 * 在spring的环境下，可以配置成单例。
 * Producer的key是一个很重要的内容，相同的key，不同的key
 *      - 可以通过它来实现负载均衡。
 *      - 合理的key设计，可以让Flink、Spark Streaming之类的实时分析工具做更快的处理
 * ack all，Kafka层面做好了只有一次消息的投递保障，但是如果真的不想丢失数据，最好自行处理
 * try{
 *     producer.send()
 * }catch(Exception e){
 *     save to redis, es ....
 * }
 */
public class ProducerSample {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //producerSend();

        //producerSyncSend();

        //producerSendWithCallback();

        producerSendWithCallbackAndPartitioner();
    }

    /**
     * 异步发送带回调和自定义的partion负载均衡。
     */
    public static void producerSendWithCallbackAndPartitioner(){
        Properties properties = new Properties();
        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");

        //重要的配置：
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, aliyun_beidou_kafka);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //指定partitioner
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, SamplePartition.class.getName());

        //Producer对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        //消息对象
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(AdminSample.TOPIC_LJC, "k-" + i,
                    "value-" + i);
            // 发送
            producer.send(record, (recordMetadata, e) -> {
                System.out.println("partition:" + recordMetadata.partition() + " " + "offset:" + recordMetadata.offset());
                /**
                 * 同一个partition下offset是【唯一】的。
                 * partition:0 offset:5
                 * partition:0 offset:6
                 * partition:2 offset:8
                 * partition:2 offset:9
                 * partition:2 offset:10
                 * partition:2 offset:11
                 * partition:1 offset:7
                 * partition:1 offset:8
                 * partition:1 offset:9
                 * partition:1 offset:10
                 */
            });
        }


        // !!【所有的通道都需要关闭】
        producer.close();
    }

    /**
     * 异步发送带回调。
     */
    public static void producerSendWithCallback(){
        Properties properties = new Properties();
        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");

        //重要的配置：
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, aliyun_beidou_kafka);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //Producer对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        //消息对象
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(AdminSample.TOPIC_LJC, "k" + i,
                    "value" + i);
            // 发送
            producer.send(record, (recordMetadata, e) -> {
                System.out.println("partition:" + recordMetadata.partition() + " " + "offset:" + recordMetadata.offset());
                /**
                 * 同一个partition下offset是【唯一】的。
                 * partition:0 offset:5
                 * partition:0 offset:6
                 * partition:2 offset:8
                 * partition:2 offset:9
                 * partition:2 offset:10
                 * partition:2 offset:11
                 * partition:1 offset:7
                 * partition:1 offset:8
                 * partition:1 offset:9
                 * partition:1 offset:10
                 */
            });
        }


        // !!【所有的通道都需要关闭】
        producer.close();
    }

    /**
     * 异步阻塞发送。
     */
    public static void producerSyncSend() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");

        //重要的配置：
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, aliyun_beidou_kafka);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //Producer对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        //消息对象
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(AdminSample.TOPIC_LJC, "key-" + i,
                    "value-" + i);
            // 发送
            final Future<RecordMetadata> send = producer.send(record);
            //【异步阻塞发送】即所谓的同步发送。
            final RecordMetadata recordMetadata = send.get();
            System.out.println("partition:" + recordMetadata.partition() + " " + "offset:" + recordMetadata.offset());
            /**
             * partition:1 offset:4
             * partition:0 offset:2
             * partition:2 offset:4
             * partition:2 offset:5
             * partition:0 offset:3
             * partition:2 offset:6
             * partition:2 offset:7
             * partition:1 offset:5
             * partition:1 offset:6
             * partition:0 offset:4
             */
        }


        // !!【所有的通道都需要关闭】
        producer.close();
    }

    /**
     * producer异步发送演示
     */
    public static void producerSend(){
        Properties properties = new Properties();
        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");

        //重要的配置：
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, aliyun_beidou_kafka);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //Producer对象
        Producer<String, String> producer = new KafkaProducer<>(properties);

        //消息对象
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(AdminSample.TOPIC_LJC, "k" + i,
                    "value" + i);
            // 发送
            producer.send(record);
        }


        // !!【所有的通道都需要关闭】
        producer.close();
    }
}
