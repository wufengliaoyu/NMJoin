package edu.hit.ftcl.wqh.common.fortest;

import edu.hit.ftcl.wqh.common.fortest.forTpcH.TpcHParameters;

/**
 * 用于进行测试数据生成的相关配置
 */
public class CommonTestDataGenerateParameters {
    //模拟数据源Zipf产生的键值数量，与读取文件数据源相对，二者取其一
    public static final int SIMULATE_SOURCE_KEY_NUM = 100000;
    //与测试数据源的数据生成有关
    //R流与S流的Zipf分布生成的键值数量(当进行模拟数据源测试与文件数据源测试时不一样)
    public static final int ZIPF_SOURCE_R_KEY_VARIETY_NUM = TpcHParameters.MAX_DAYS;
    public static final int ZIPF_SOURCE_S_KEY_VARIETY_NUM = TpcHParameters.MAX_DAYS;
    //R流与S流的Zipf分布的倾斜程度
    public static final double ZIPF_SOURCE_R_SKEW = 0.2;
    public static final double ZIPF_SOURCE_S_SKEW = 0.2;
    //R流与S流数据源每秒钟生成的数据数量
//    public static final long ZIPF_SOURCE_R_GENERATE_NUM_PER_SECOND = 100000;
//    public static final long ZIPF_SOURCE_S_GENERATE_NUM_PER_SECOND = 100000;

    //R流与S流数据分布的变化周期（单位：毫秒）
    public static final long ZIPF_R_DISTRIBUTE_PERIOD_MILLISECOND = 3*60*1000;
    public static final long ZIPF_S_DISTRIBUTE_PERIOD_MILLISECOND = 3*60*1000;
    //R流与S流数据分布在每个周期内变化多少（即每个键值偏移占总键值数量的多少）（是一个小数，表示占比;0表示不开启负载变化）
    public static final double ZIPF_R_DISTRIBUTE_PER_PERIOD_CHANGE_PROPORTION = 0.0;
    public static final double ZIPF_S_DISTRIBUTE_PER_PERIOD_CHANGE_PROPORTION = 0.0;

    //kafka中两个主题的分区数量，这决定着数据生成程序的sink并行度以及数据读取过程中的source以及转换map的并行度
    public static final int KAFKA_R_TOPIC_PARTITIONS_NUM = 3;
    public static final int KAFKA_S_TOPIC_PARTITIONS_NUM = 3;

    //在用数据源的输入速率随时间发生改变的实验中使用
    //用于测试数据源通知 Monitor 信息的路径，主要是通知当前产生数据周期结束以及下一个产出数据周期开始
    public static final String FIRST_PRODUCER_NOTICE_MONITOR_MESSAGE = "/MKJoin/Monitor/FirstProducerMessage";
    public static final String SECOND_PRODUCER_NOTICE_MONITOR_MESSAGE = "/MKJoin/Monitor/SecondProducerMessage";


}
