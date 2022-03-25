package edu.hit.ftcl.wqh.nomigrationjoin;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 将NMJoin与分组分区方法结合，例如上游NM分组为16，每组节点为4，则传输到此处的元组的分区数量为16，
 *      之后此转换函数将要发往每个分区的元组分到组内的四个节点当中，此时分为两种情况：
 *      （1）若为存储元组，则随机发给组内的一个节点存储，之后发往每一个节点进行连接
 *      （2）若为连接元组，则发往组内的所有节点进行连接
 * @param <F> 第一个输入源的类型
 * @param <S> 第二个输入源的类型
 */
public class NMJoinToGroupRandomFlatMap<F, S> extends RichFlatMapFunction<NMJoinUnionType<F, S>, NMJoinUnionType<F, S>> {

    //用于计算随机组内存储joiner节点的随机数生成器
    private Random random;

    @Override
    public void open(Configuration parameters) throws Exception {
        //输出一些当前系统配置信息
        System.out.println("NM-CR-Join：当前系统Joiner实例总数为： " + NMJoinConfigParameters.JOINER_TOTAL_NUM
                + " ； 这些实例被分为 " + NMJoinConfigParameters.TOTAL_GROUP_NUM_OF_TASK
                + " 组；每组内有 " + NMJoinConfigParameters.INSTANCE_NUM_OF_EACH_GROUP
                + " 个节点。\n");
        System.out.println("R流最大键值为-"+NMJoinConfigParameters.R_ROUTE_TABLE_MAX_KEY);
        //初始化随机数生成器
        random = new Random();
    }

    @Override
    public void flatMap(NMJoinUnionType<F, S> value, Collector<NMJoinUnionType<F, S>> out) throws Exception {
        //如果每个组内只有一个节点，则直接返回输入值即可，此时就是正常的NMJoin方法
        if (NMJoinConfigParameters.INSTANCE_NUM_OF_EACH_GROUP == 1) {
            out.collect(value);
        } else {  //如果组内多个节点，则进入NM到分组分区的转换
            if (value.isStore()) {//如果是存储元组
                //当前元组要发往哪个分组
                int groupNum = value.getNumPartition();
                //随机选择一个组内编号作为存储节点
                int storePartition = groupNum * NMJoinConfigParameters.INSTANCE_NUM_OF_EACH_GROUP + random.nextInt(NMJoinConfigParameters.INSTANCE_NUM_OF_EACH_GROUP);
                //之后向组内的每一个节点发送连接元组
                for (int i = 0; i < NMJoinConfigParameters.INSTANCE_NUM_OF_EACH_GROUP; i++) {
                    //获取当前连接分区
                    int joinPartition = groupNum * NMJoinConfigParameters.INSTANCE_NUM_OF_EACH_GROUP + i;
                    //创建连接元组
                    NMJoinUnionType<F, S> joinTuple = new NMJoinUnionType<F, S>();
                    //设置连接元组的值，要连接的最大时间戳，要连接的最小时间戳，分区，存储/连接模式
                    if (value.isOne()) {   //要区分是否为第一个类型
                        joinTuple.one(value.getFirstType());
                    } else {
                        joinTuple.two(value.getSecondType());
                    }
                    joinTuple.setMaxTimestamp(Long.MAX_VALUE);   //由于之后到达的元组均会存储在该分组中，所有要连接的最大时间戳为无穷
                    joinTuple.setMinTimestamp(value.getMinTimestamp());
                    joinTuple.setNumPartition(joinPartition);    //设置分区号为当前遍历的连接分区
                    joinTuple.setOtherTimestamp(value.getOtherTimestamp());
                    joinTuple.setJoinMode();

                    //如果当前为存储分区，则向存储分区发送存储元组，其最大时间戳与输入元组的相同，用于保存该元组的时间戳
                    //如果该元组是要存储到对应的joiner中的，则将其模式改为store，并且将最大的时间戳设置为当前元组的时间戳
                    // （因为正常情况下其要连接的最大时间戳为无穷，是个固定值，因此在此可以利用该额外字段来存储额外的信息）
                    if (joinPartition == storePartition) {
                        joinTuple.setMaxTimestamp(value.getMaxTimestamp());    //设置最大时间戳保存元组时间戳
                        joinTuple.setStoreMode();
                    }

                    //输出结果
                    out.collect(joinTuple);

                }//end for

            } else {//如果是连接元组
                //当前元组要发往哪个分组
                int groupNum = value.getNumPartition();
                //之后向组内的每一个节点发送连接元组
                for (int i = 0; i < NMJoinConfigParameters.INSTANCE_NUM_OF_EACH_GROUP; i++) {
                    //获取当前连接分区
                    int joinPartition = groupNum * NMJoinConfigParameters.INSTANCE_NUM_OF_EACH_GROUP + i;
                    //创建连接元组
                    NMJoinUnionType<F, S> joinTuple = new NMJoinUnionType<F, S>();
                    //设置连接元组的值，要连接的最大时间戳，要连接的最小时间戳，分区（需要加上R节点的数量，因为S的元组存储在后几个节点当中），存储/连接模式
                    if (value.isOne()) {   //要区分是否为第一个类型
                        joinTuple.one(value.getFirstType());
                    } else {
                        joinTuple.two(value.getSecondType());
                    }
                    joinTuple.setMaxTimestamp(value.getMaxTimestamp());
                    joinTuple.setMinTimestamp(value.getMinTimestamp());
                    joinTuple.setNumPartition(joinPartition);    //设置分区号为当前遍历的连接分区
                    joinTuple.setOtherTimestamp(value.getOtherTimestamp());
                    joinTuple.setJoinMode();

                    //输出结果
                    out.collect(joinTuple);

                }//end for
            }
        }
    }
}
