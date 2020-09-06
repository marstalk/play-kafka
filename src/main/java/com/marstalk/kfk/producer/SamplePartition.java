package com.marstalk.kfk.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 消息走哪个分区，是由producer来决定的，
 * 而开发者可以通过提供自定义的Partitioner来实现自己的逻辑。
 * @author ljc524
 */
public class SamplePartition implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //这个方法决定了消息进入哪个分区

        /**
         * 例子中，key是这样子分布的：
         * k-1
         * k-2
         * k-3
         * ....
         * k-10
         * 现在要求，根据key的余数来区分区。
         */
        String keyStr = key + "";
        final String keyInt = keyStr.substring(2);
        System.out.println("keyStr:" + keyStr + " keyInt:" + keyInt);
        int i = Integer.parseInt(keyInt);

        //当前分区有三个
        return i % 3;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
