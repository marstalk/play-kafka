package com.marstalk.kfk.consumer;

/**
 * 每个线程对应一个KafkaConsumer实例，解决线程安全问题。
 * 业务性数据，在失败的时候可以rollback。数据不能丢失，类似TCP
 *
 *
 */
public class ConsumerThreadSample {
}
