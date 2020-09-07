package com.marstalk.kfk.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class StreamSample {
    private static final String INPUT_TOPIC = "ljc-stream-in";
    private static final String OUTPUT_TOPIC = "ljc-stream-out";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getProperty("aliyun_beidou_kafka"));
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ljc-paly-kafka");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //【重点】如何构建流结构拓扑
        StreamsBuilder streamBuilder = new StreamsBuilder();
        //构建word processor
        //wordCountStream(streamBuilder);
        //构建foreach
        foreachStream(streamBuilder);
        KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder.build(), props);

        kafkaStreams.start();
    }

    /**
     * 如何定义流计算过程
     *
     * @param streamsBuilder
     */
    static void foreachStream(final StreamsBuilder streamsBuilder) {
        KStream<String, String> kStreamSource = streamsBuilder.stream(INPUT_TOPIC);


        kStreamSource.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                .foreach((key, value) -> System.out.println(key + " + " + value));

    }

    /**
     * 如何定义流计算过程
     *
     * @param streamsBuilder
     */
    static void wordCountStream(final StreamsBuilder streamsBuilder) {
        //KStream<Key, Value> 不断从INPUT_TOPIC上获取新数据，并且追加到流上的抽象都想
        KStream<String, String> kStreamSource = streamsBuilder.stream(INPUT_TOPIC);

        //KTable是数据集合的抽象对象
        //【算子】
        final KTable<String, Long> count =
                //flatMapValues将每一个消息通过空格进行拆分，即一个消息拆为多个消息。
                //groupBy分组（即合并），以value作为合并。
                //统计出现次数。
                kStreamSource.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
                        .groupBy((key, value) -> value)
                        .count();

        //将结果输出到OUT_TOPIC
        count.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

}
