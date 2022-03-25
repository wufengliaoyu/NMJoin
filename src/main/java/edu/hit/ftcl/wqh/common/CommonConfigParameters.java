package edu.hit.ftcl.wqh.common;

public class CommonConfigParameters {
    //zookeeper集群地址
    public static final String CONNECT_STRING = "instance1:2181,instance2:2181,instance3:2181";
    //会话过期时间，即与集群断开超过该时间则判定当前会话结束
    public static final int SESSION_TIMEOUT = 400000;

    //用于定义水位线晚于最大时间戳的时间长度（单位：ms）
    public static final long WATERMARK_AFTER_MAX_TIMESTAMP = 300L;

    //kafka集群列表
    public static final String KAFKA_BROKER_LIST = "instance1:9092,instance2:9092,instance3:9092";
    //用于测试的两个kafka实时主题
    public static final String FIRST_KAFKA_REAL_TIME_TEST_TOPIC = "FirstInputTestTopicV1";
    public static final String SECOND_KAFKA_REAL_TIME_TEST_TOPIC = "SecondInputTestTopicV1";

    //用于收集结果的kafka主题
    public static final String TEST_RESULTS_COLLECT_KAFKA_TOPIC = "TestLatencyResultTopicV1";
    //Kafka结果收集主题的分区数量，该值用于确定Flink中sink算子的并行度（设置太多磁盘容量不够）
    public static final int KAFKA_RESULTS_COLLECT_TOPIC_PARTITIONS_NUM = 3;

}
