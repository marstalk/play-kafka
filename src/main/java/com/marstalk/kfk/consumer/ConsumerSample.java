package com.marstalk.kfk.consumer;

import com.marstalk.kfk.admin.AdminSample;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerSample {

    public static void main(String[] args) {
        //helloWorld();

        //committedOffset();

        committedOffsetWithPartition();

        //committedOffsetWithSubscribeSpecificPartition();
    }

    /**
     * 手动提交offset，并且指定消费某个topic下的partition
     */
    private static void committedOffsetWithSubscribeSpecificPartition() {
        Properties props = new Properties();

        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");
        props.setProperty("bootstrap.servers", aliyun_beidou_kafka);

        /**
         * 只要有相同的groupId，那么该消费者在相同的分组里。
         */
        props.setProperty("group.id", "test");
        //【auto.commit = false】
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        TopicPartition p0 = new TopicPartition(AdminSample.TOPIC_LJC, 0);
        TopicPartition p1 = new TopicPartition(AdminSample.TOPIC_LJC, 1);
        TopicPartition p2 = new TopicPartition(AdminSample.TOPIC_LJC, 2);

        //订阅哪个或者哪些topic
        //consumer.subscribe(Arrays.asList(AdminSample.TOPIC_LJC));

        //只订阅某个topic的某个分区
        consumer.assign(Arrays.asList(p0));

        while (true) {
            //单个consumer【本例子中，只有一个consumer】，会消费所有partition的消息。
            //默认【从头开始】【顺序】消费。
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

            //每个partition单独处理。
            for (TopicPartition partition : records.partitions()) {
                final List<ConsumerRecord<String, String>> partionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partionRecords) {
                    //想把数据保存到数据库或者其他的操作。
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    //如果失败则回滚
                }
                //本次消费的index=size-1//TODO offset是累加的，但是size并不是，这里应该不对吧？？
                long lastOffset = partionRecords.size() - 1;
                System.out.println(">>>>>>>>>>>>>lastOffset:" + lastOffset);
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                //要把下一次消费的index提交给kafka server，所以要 +1
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));
                //commitSync VS commitSync
                //如果成功，则手动通知offset提交，每个partition单独提交offset。
                consumer.commitSync(offset);
                System.out.println(">>>>>>>>>>>>>>partition:" + partition + " end >>>>>>>>>>>>>>");
            }
        }
    }

    /**
     * 手动提交offset，并且循环partition消费
     */
    private static void committedOffsetWithPartition() {
        Properties props = new Properties();

        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");
        props.setProperty("bootstrap.servers", aliyun_beidou_kafka);

        /**
         * 只要有相同的groupId，那么该消费者在相同的分组里。
         */
        props.setProperty("group.id", "test");
        //【auto.commit = false】
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        //订阅哪个或者哪些topic
        consumer.subscribe(Arrays.asList(AdminSample.TOPIC_LJC));
        while (true) {
            //单个consumer【本例子中，只有一个consumer】，会消费所有partition的消息。
            //默认【从头开始】【顺序】消费。
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

            //每个partition单独处理。
            for (TopicPartition partition : records.partitions()) {
                final List<ConsumerRecord<String, String>> partionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partionRecords) {
                    //想把数据保存到数据库或者其他的操作。
                    System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                    //如果失败则回滚
                }
                //本次消费的index=size-1//TODO offset是累加的，但是size并不是，这里应该不对吧？？
                long lastOffset = partionRecords.size() - 1;
                Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                //要把下一次消费的index提交给kafka server，所以要 +1
                offset.put(partition, new OffsetAndMetadata(lastOffset + 1));
                //commitSync VS commitSync
                //如果成功，则手动通知offset提交，每个partition单独提交offset。
                consumer.commitSync(offset);
                System.out.println(">>>>>>>>>>>>>>partition:" + partition + " end >>>>>>>>>>>>>>");
            }
        }
    }

    /**
     * 手动提交offset
     */
    private static void committedOffset() {
        Properties props = new Properties();

        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");
        props.setProperty("bootstrap.servers", aliyun_beidou_kafka);

        /**
         * 只要有相同的groupId，那么该消费者在相同的分组里。
         */
        props.setProperty("group.id", "test");
        //【auto.commit = false】
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        //订阅哪个或者哪些topic
        consumer.subscribe(Arrays.asList(AdminSample.TOPIC_LJC));
        while (true) {
            //单个consumer【本例子中，只有一个consumer】，会消费所有partition的消息。
            //默认【从头开始】【顺序】消费。
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                //想把数据保存到数据库//TODO
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
                //如果失败则回滚
            }
            //如果成功，则手动通知offset提交
            consumer.commitAsync();
        }
    }

    /**
     * 工作中有这种用法，【自动提交offset】但是不推荐。
     */
    private static void helloWorld() {
        Properties props = new Properties();

        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");
        props.setProperty("bootstrap.servers", aliyun_beidou_kafka);
        props.setProperty("group.id", "test");
        //自动提交offset
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);
        //订阅哪个或者哪些topic
        consumer.subscribe(Arrays.asList(AdminSample.TOPIC_LJC));
        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}
