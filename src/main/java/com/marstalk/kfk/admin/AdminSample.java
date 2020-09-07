package com.marstalk.kfk.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class AdminSample {
    public static final String TOPIC_LJC = "liujc_topic";
    public static final String TOPIC_LJC_TWO = "liujc_topic_two";


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //AdminClient adminClient = adminClient();
        //System.out.println(adminClient);


        createTopic();

        //deleteTopic(TOPIC_LJC_TWO);

        listAllTopics();

        //increPartitions();

        //describeTopics();

        //alterConfig();


        //describeConfigs();

    }

    /**
     * 增加partition数
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private static void increPartitions() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        Map<String, NewPartitions> partitionsMap = new HashMap<>();
        //将原来的1个partitions增加到3
        NewPartitions newPartitions = NewPartitions.increaseTo(3);
        //指定特定的topic
        partitionsMap.put(TOPIC_LJC, newPartitions);
        CreatePartitionsResult partitions = adminClient.createPartitions(partitionsMap);
        System.out.println(partitions.all().get());
    }

    private static void alterConfig() {
        AdminClient adminClient = adminClient();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_LJC);

        //演示了这个常用的配置修改。
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate", "true")));
        Map<ConfigResource, Config> map = new HashMap<>();
        map.put(configResource, config);

        //单机版演示，所以用了这个过期的方法。
        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(map);
        System.out.println(alterConfigsResult);


        //这个对单机版本支持比较差劲，暂时不用。
        //adminClient.incrementalAlterConfigs()
    }


    /**
     * 主要用于监控。
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private static void describeConfigs() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();

//        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, TOPIC_LJC);
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_LJC);
        List<ConfigResource> resources = Arrays.asList(configResource);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(resources);
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        configResourceConfigMap.forEach((configResource1, config) -> {
            System.out.println("configResource + " + configResource1 + " : " + "config" + config);
        });
        /**
         * configResource + ConfigResource(type=TOPIC, name='liujc_topic') : configConfig(
         *      entries=[
         *          ConfigEntry(
         *              name=compression.type,
         *              value=producer,
         *              source=DEFAULT_CONFIG,
         *              isSensitive=false,
         *              isReadOnly=false,
         *              synonyms=[]),
         *          ConfigEntry(
         *              name=leader.replication.throttled.replicas,
         *              value=,
         *              source=DEFAULT_CONFIG,
         *              isSensitive=false,
         *              isReadOnly=false, synonyms=[]),
         *          ConfigEntry(
         *              name=message.downconversion.enable,
         *              value=true,
         *              source=DEFAULT_CONFIG,
         *              isSensitive=false,
         *              isReadOnly=false,
         *              synonyms=[]), ConfigEntry(name=min.insync.replicas, value=1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.jitter.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=cleanup.policy, value=delete, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=follower.replication.throttled.replicas, value=, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.bytes, value=1073741824, source=STATIC_BROKER_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=flush.messages, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.format.version, value=2.4-IV1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=file.delete.delay.ms, value=60000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.compaction.lag.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=max.message.bytes, value=1000012, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=min.compaction.lag.ms, value=0, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.type, value=CreateTime, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]),
         *          ConfigEntry(
         *              name=preallocate,
         *              value=false,
         *              source=DEFAULT_CONFIG,
         *              isSensitive=false,
         *              isReadOnly=false,
         *              synonyms=[]), ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=index.interval.bytes, value=4096, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=unclean.leader.election.enable, value=false, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=retention.bytes, value=-1, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=delete.retention.ms, value=86400000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.ms, value=604800000, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[]), ConfigEntry(name=segment.index.bytes, value=10485760, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, synonyms=[])])
         */
    }

    /**
     * 主要用于监控。
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private static void describeTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_LJC));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
        stringTopicDescriptionMap.forEach((s, topicDescription) -> System.out.println(s + ": " + topicDescription));
        /**
         * liujc_topic: (
         *      name=liujc_topic,
         *      internal=false,
         *      partitions=(partition=0,
         *             leader=47.107.166.120:9092 (id: 0 rack: null),
         *             replicas=47.107.166.120:9092 (id: 0 rack: null),
         *             isr=47.107.166.120:9092 (id: 0 rack: null)
         *             ),
         *             authorizedOperations=[]
         *      )
         */
    }

    public static void deleteTopic(String topic) {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topic));
        System.out.println(deleteTopicsResult);
    }

    /**
     * 获取所有的主题(不包括内部的）
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void listAllTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();

        //是否查询internal的topic
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        //ListTopicsResult listTopicsResult = adminClient.listTopics();
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);

        //future对象。
        KafkaFuture<Set<String>> kafkaFuture = listTopicsResult.names();
        Set<String> strings = kafkaFuture.get();
        strings.forEach(System.out::println);
        /**
         * ljc-topic
         * liujc_topic
         * __consumer_offsets
         */

        //打印TopicListing
        KafkaFuture<Collection<TopicListing>> listings = listTopicsResult.listings();
        listings.get().stream().forEach(topicListing -> System.out.println(topicListing));
        /**
         * (name=ljc-topic, internal=false)
         * (name=liujc_topic, internal=false)
         * (name=__consumer_offsets, internal=true)
         */

        //打印
        KafkaFuture<Map<String, TopicListing>> mapKafkaFuture = listTopicsResult.namesToListings();
        mapKafkaFuture.get().forEach((s, topicListing) -> System.out.println(s + ": " + topicListing));
        /**
         * ljc-topic: (name=ljc-topic, internal=false)
         * liujc_topic: (name=liujc_topic, internal=false)
         * __consumer_offsets: (name=__consumer_offsets, internal=true)
         */
    }

    /**
     * 创建topic
     * 需要指定分区大小和副本因子。
     * 【可重复创建，而不报错】
     */
    public static void createTopic() {
        AdminClient adminClient = adminClient();
        NewTopic topic = new NewTopic(TOPIC_LJC, 1, new Short("1"));
        //NewTopic topic2 = new NewTopic(TOPIC_LJC_TWO, 1, new Short("1"));
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(topic));
        System.out.println(createTopicsResult);
    }

    /**
     * 通过ip:port获取AdminClient实例。
     *
     * @return
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        String aliyun_beidou_kafka = System.getProperty("aliyun_beidou_kafka");
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, aliyun_beidou_kafka);
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

}
