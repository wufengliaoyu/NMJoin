package edu.hit.ftcl.wqh.common;

import edu.hit.ftcl.wqh.common.fortest.CommonTestDataGenerateParameters;
import edu.hit.ftcl.wqh.nomigrationjoin.NMJoinUnionType;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class CommonJoinUnionStream<F,S> {
    //用于存储两个要连接的数据流
    protected DataStream<F> firstStream;
    protected DataStream<S> secondStream;

    //两个数据流的键值提取器，键值为Double类型
    protected KeySelector<F,Double> keySelector_R;
    protected KeySelector<S,Double> keySelector_S;

    /**
     * 有参构造器，为各个属性赋初始值
     */
    public CommonJoinUnionStream(DataStream<F> firstStream, DataStream<S> secondStream, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S) {
        this.firstStream = firstStream;
        this.secondStream = secondStream;
        this.keySelector_R = keySelector_R;
        this.keySelector_S = keySelector_S;
    }


    /**
     * 将两个输入的数据流包装成元组类型相同的数据流,
     *     在此会设置OtherTimestamp为当前时间，整个join的处理流程为：source->map->map->router->joiner ,
     *     这相当于进入第二个map的时间，可以用来在joiner后记录一个时间后相减，便可得到延迟
     * @param firstInputStream 第一个输入流
     * @param secondInputStream 第二个输入流
     * @return
     */
    protected DataStream<NMJoinUnionType<F, S>> unionTwoInputStream(DataStream<F> firstInputStream, DataStream<S> secondInputStream) {
        //包装两个输入流为相同的类型
        //将第一个流包装成NMJoinUnionType<F, S>类型的流
        DataStream<NMJoinUnionType<F, S>> inputStream1 = firstInputStream.map(new MapFunction<F, NMJoinUnionType<F, S>>() {
            public NMJoinUnionType<F, S> map(F value) throws Exception {
                NMJoinUnionType<F, S> firstUnionType = new NMJoinUnionType<F, S>();
                firstUnionType.one(value);

                //TODO 测试时间数据设置，（此时不采用高性能的获取时间戳方式）
                firstUnionType.setOtherTimestamp(System.currentTimeMillis());
//                firstUnionType.setOtherTimestamp(CurrentTimeMillisClock.getInstance().now());

                return firstUnionType;
            }
        }).returns(new TypeHint<NMJoinUnionType<F, S>>() {
            @Override
            public TypeInformation<NMJoinUnionType<F, S>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).setParallelism(CommonTestDataGenerateParameters.KAFKA_R_TOPIC_PARTITIONS_NUM);    //TODO 并行度与数据源相同


        //将第二个流包装成NMJoinUnionType<F, S>类型的流
        DataStream<NMJoinUnionType<F, S>> inputStream2 = secondInputStream.map(new MapFunction<S, NMJoinUnionType<F, S>>() {
            public NMJoinUnionType<F, S> map(S value) throws Exception {
                NMJoinUnionType<F, S> secondUnionType = new NMJoinUnionType<F, S>();
                secondUnionType.two(value);

                //TODO 测试时间数据设置，（此时不采用高性能的获取时间戳方式）
                secondUnionType.setOtherTimestamp(System.currentTimeMillis());
//                secondUnionType.setOtherTimestamp(CurrentTimeMillisClock.getInstance().now());

                return secondUnionType;
            }
        }).returns(new TypeHint<NMJoinUnionType<F, S>>() {
            @Override
            public TypeInformation<NMJoinUnionType<F, S>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).setParallelism(CommonTestDataGenerateParameters.KAFKA_S_TOPIC_PARTITIONS_NUM);    //TODO 并行度与数据源相同


        //合并两个输入流为一个流
        return inputStream1.union(inputStream2);
    }

    /**
     * 范围连接
     * @param R_TimeWindows R窗口中保存元组的时间
     * @param S_TimeWindows S窗口中保存元组的时间
     * @param r_behind_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
     * @param r_surpass_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
     * @return 连接之后满足结果的输出流,该流中存储的元组类型为联合类型
     */
    public abstract DataStream<NMJoinUnionType<F,S>> bandNMJoinWithTimeWindow(Time R_TimeWindows, Time S_TimeWindows, double r_surpass_S, double r_behind_S);


    /**
     * 实施不同连接的框架，只有Router与Joiner不同
     * @param router 不同的路由方法
     * @param joiner 不同的连接方法
     * @param numOfRouter 路由节点的数量
     * @param numOfJoiner 连接节点的数量
     * @return 结果数据流
     */
    protected DataStream<NMJoinUnionType<F, S>> joinMethodWithDifferentImplement(CommonRouter<F, S> router,
                                                                                 CommonJoiner<F, S> joiner,
                                                                                 int numOfRouter,
                                                                                 int numOfJoiner) {
        //合并两个数据流
        DataStream<NMJoinUnionType<F, S>> unionStream = unionTwoInputStream(firstStream, secondStream);

        //对合并后的数据流进行连接
        return unionStream.process(router)                                          //路由
                .setParallelism(numOfRouter)                                        //设置Router并行度
                .partitionCustom(new Partitioner<Integer>() {                       //自定义分区器
                    public int partition(Integer key, int numPartitions) {
                        return key;
                    }
                }, new KeySelector<NMJoinUnionType<F, S>, Integer>() {
                    public Integer getKey(NMJoinUnionType<F, S> value) throws Exception {
                        return value.getNumPartition();
                    }
                })
                .process(joiner)                                                    //连接
                .setParallelism(numOfJoiner);                                       //设置Joiner并行度
    }



}
