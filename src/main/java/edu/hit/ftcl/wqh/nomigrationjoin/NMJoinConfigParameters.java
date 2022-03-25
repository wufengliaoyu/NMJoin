package edu.hit.ftcl.wqh.nomigrationjoin;

import edu.hit.ftcl.wqh.common.fortest.CommonTestDataGenerateParameters;

/**
 * 用于保存集群所有的相关初始化配置，这些配置在程序运行之前应该都已经被确定，并且在程序运行期间不可以改变
 * TODO 之后如果有需要可以改成在程序中动态的配置
 */
public class NMJoinConfigParameters {
    //zookeeper集群地址
    public static final String CONNECT_STRING = "instance1:2181,instance2:2181,instance3:2181";
    //会话过期时间，即与集群断开超过该时间则判定当前会话结束
    public static final int SESSION_TIMEOUT = 60000;

    //zookeeper中用于保存所有前缀节点路径，这些节点为辅助节点，用于清晰的管理节点结构，但必须事先创建
    public static final String PREFIX_NMJOIN_ROOT_PATH = "/NMJoin";
    public static final String PREFIX_NMJOIN_ROUTER_PATH = "/NMJoin/Router";
    public static final String PREFIX_NMJOIN_JOINER_PATH = "/NMJoin/Joiner";
    public static final String PREFIX_NMJOIN_MONITOR_PATH = "/NMJoin/Monitor";

    //zookeeper中用于保存R与S的最新同步存储路由表的节点
    public static final String BOTH_STORE_ROUTE_TABLE_PATH = "/NMJoin/Router/RRouteTable";
//    public static final String S_STORE_ROUTE_TABLE_PATH = "/NMJoin/Router/SRouteTable";
    //zookeeper中用于保存R与S的上一个周期结束时的同步存储路由表的节点（用于Router节点更新内部的连接路由表）
    public static final String OLD_R_STORE_ROUTE_TABLE_PATH = "/NMJoin/Router/OldRRouteTable";
    public static final String OLD_S_STORE_ROUTE_TABLE_PATH = "/NMJoin/Router/OldSRouteTable";

    //zookeeper中用于保存R与S的同步水位线的节点
    public static final String SYNC_MIN_WATERMARK_PATH = "/NMJoin/Router/RMinWatermark";
    public static final String SYNC_MAX_WATERMARK_PATH = "/NMJoin/Router/SMinWatermark";

    //zookeeper中用于通知所有Router同步存储路由表的标记节点
    public static final String SYNC_ROUTER_STORE_TABLE_FLAG = "/NMJoin/Router/SyncRouterFlag";
    //zookeeper中各个Router用于报告自身同步存储路由表已经完成的标记节点，后阶各个Router任务编号
    public static final String EACH_ROUTER_SYNC_COMPLETE_FLAG = "/NMJoin/Router/CompleteFlagOfRouter";

    //zookeeper中用于通知所有Joiner上传各自负载的标记节点
    public static final String SYNC_JOINER_UPLOAD_FLAG = "/NMJoin/Router/JoinerUploadFlag";
    //zookeeper中各个Joiner用于报告自身负载上传已经完成的标记节点,后接各个Joiner任务编号
    public static final String EACH_JOINER_UPLOAD_COMPLETE_FLAG = "/NMJoin/Router/UploadCompleteFlagOfJoiner";


    //用于定义水位线晚于最大时间戳的时间长度（单位：ms）
    public static final long WATERMARK_AFTER_MAX_TIMESTAMP = 300L;


    //将NMJoin与分组分区方法结合，用于设置NMJoin中一共有多少个分组
    public static final int TOTAL_GROUP_NUM_OF_TASK = 16;
    //设置一个分组当中有多少个节点
    public static final int INSTANCE_NUM_OF_EACH_GROUP = 4;
    //根据上面两项计算出一共有多少个Joiner节点
    public static final int JOINER_TOTAL_NUM = TOTAL_GROUP_NUM_OF_TASK * INSTANCE_NUM_OF_EACH_GROUP;

    //Router的数量
    public static final int NUM_OF_ROUTER = 64;

    //R的存储路由表范围hash后的键值数量
    public static final int R_ROUTE_TABLE_KEY_NUM = 1280;
    //R的存储路由表中所有元组的最大键值以及最小键值
    public static final double R_ROUTE_TABLE_MAX_KEY = CommonTestDataGenerateParameters.ZIPF_SOURCE_R_KEY_VARIETY_NUM;
    public static final double R_ROUTE_TABLE_MIN_KEY = 1.0;

    //S的存储路由表范围hash后的键值数量
    public static final int S_ROUTE_TABLE_KEY_NUM = 1280;
    //S的存储路由表中所有元组的最大键值以及最小键值
    public static final double S_ROUTE_TABLE_MAX_KEY = CommonTestDataGenerateParameters.ZIPF_SOURCE_S_KEY_VARIETY_NUM;
    public static final double S_ROUTE_TABLE_MIN_KEY = 1.0;

    //中央协调线程重构路由表的周期（ms）
    public static final long COORDINATE_PERIOD = 10*1000L;

    //该值是Joiner生成新子树以及过期旧子树的周期(ms)
    public static final long JOINER_CHANGE_LOCAL_SUB_TREE_PERIOD = 30*1000L;

    //进行负载不平衡判断时，键值负载的分布与上一次调整时的键值负载分布之间的变化超过该阈值时，才会进行后面的负载不平衡判断，
    // 这样做的目的是防止频繁的存储路由表调整
    public static final double DISTRIBUTE_CHANGE_THRESHOLD = 0.2;
    //进行负载平衡时的阈值
    public static final double WORKLOAD_BALANCE_THRESHOLD = 0.2;

    //设置是否进行负载平衡(true表示开启负载平衡)
    public static final boolean IS_WORKLOAD_BALANCE_TURN_ON = true;

    //与测试数据源的数据生成有关
    //R流与S流的Zipf分布生成的键值数量
    public static final int ZIPF_SOURCE_R_KEY_VARIETY_NUM = CommonTestDataGenerateParameters.ZIPF_SOURCE_R_KEY_VARIETY_NUM;
    public static final int ZIPF_SOURCE_S_KEY_VARIETY_NUM = CommonTestDataGenerateParameters.ZIPF_SOURCE_S_KEY_VARIETY_NUM;
    //R流与S流的Zipf分布的倾斜程度
    public static final double ZIPF_SOURCE_R_SKEW = CommonTestDataGenerateParameters.ZIPF_SOURCE_R_SKEW;
    public static final double ZIPF_SOURCE_S_SKEW = CommonTestDataGenerateParameters.ZIPF_SOURCE_S_SKEW;
    //R流与S流数据源每秒钟生成的数据数量
//    public static final long ZIPF_SOURCE_R_GENERATE_NUM_PER_SECOND = CommonTestDataGenerateParameters.ZIPF_SOURCE_R_GENERATE_NUM_PER_SECOND;
//    public static final long ZIPF_SOURCE_S_GENERATE_NUM_PER_SECOND = CommonTestDataGenerateParameters.ZIPF_SOURCE_S_GENERATE_NUM_PER_SECOND;

    //R流与S流数据分布的变化周期（单位：毫秒）
    public static final long ZIPF_R_DISTRIBUTE_PERIOD_MILLISECOND = CommonTestDataGenerateParameters.ZIPF_R_DISTRIBUTE_PERIOD_MILLISECOND;
    public static final long ZIPF_S_DISTRIBUTE_PERIOD_MILLISECOND = CommonTestDataGenerateParameters.ZIPF_S_DISTRIBUTE_PERIOD_MILLISECOND;
    //R流与S流数据分布在每个周期内变化多少（即每个键值偏移占总键值数量的多少）（是一个小数，表示占比）
    public static final double ZIPF_R_DISTRIBUTE_PER_PERIOD_CHANGE_PROPORTION = CommonTestDataGenerateParameters.ZIPF_R_DISTRIBUTE_PER_PERIOD_CHANGE_PROPORTION;
    public static final double ZIPF_S_DISTRIBUTE_PER_PERIOD_CHANGE_PROPORTION = CommonTestDataGenerateParameters.ZIPF_S_DISTRIBUTE_PER_PERIOD_CHANGE_PROPORTION;



    //与监控joiner存储的元组数量有关的zookeeper路径
    //用于通知各个Joiner节点上传各自的负载（目前负载定义为存储的元组数量）
    public static final String MONITOR_NOTICE_JOINER_LOAD_UPDATE_FLAG = "/NMJoin/Monitor/JoinerLoadUpdateFlag";
    //用于存储各个joiner上传的负载的zookeeper上的节点路径，后面需要接上各个joiner的标记
    public static final String MONITOR_EACH_JOIN_LOAD = "/NMJoin/Monitor/EachJoinerLoad";








}
