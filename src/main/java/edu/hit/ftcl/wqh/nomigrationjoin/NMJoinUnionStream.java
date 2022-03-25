package edu.hit.ftcl.wqh.nomigrationjoin;

import edu.hit.ftcl.wqh.common.fortest.CommonTestDataGenerateParameters;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 同时保存了两个数据流数据的合并数据流，所有连接的方法均由该类提供
 * @param <F> 第一个流的泛型
 * @param <S> 第二个流的泛型
 */
public class NMJoinUnionStream<F,S> {
    //用于存储两个要连接的数据流
    private DataStream<F> firstStream;
    private DataStream<S> secondStream;

    //两个数据流的键值提取器，键值为Double类型
    private KeySelector<F,Double> keySelector_R;
    private KeySelector<S,Double> keySelector_S;

    /**
     * 有参构造器，为各个属性赋初始值
     */
    public NMJoinUnionStream(DataStream<F> firstStream, DataStream<S> secondStream, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S) {
        this.firstStream = firstStream;
        this.secondStream = secondStream;
        this.keySelector_R = keySelector_R;
        this.keySelector_S = keySelector_S;
    }

    /**
     * 范围连接
     * @param R_TimeWindows R窗口中保存元组的时间
     * @param S_TimeWindows S窗口中保存元组的时间
     * @param r_behind_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
     * @param r_surpass_S 用于范围连接（以一个S元组的时间为参照物），即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
     * @return 连接之后满足结果的输出流,该流中存储的元组类型为联合类型
     */
    public DataStream<NMJoinUnionType<F,S>> bandNMJoinWithTimeWindow(Time R_TimeWindows, Time S_TimeWindows,double r_surpass_S, double r_behind_S){

        /************************************************包装两个输入流为相同的类型******************************************************/
        //将第一个流包装成NMJoinUnionType<F, S>类型的流
        DataStream<NMJoinUnionType<F, S>> inputStream1 = firstStream.map(new MapFunction<F, NMJoinUnionType<F, S>>() {
            public NMJoinUnionType<F, S> map(F value) throws Exception {
                NMJoinUnionType<F, S> fsnmJoinUnionType = new NMJoinUnionType<F, S>();
                fsnmJoinUnionType.one(value);

                //TODO 测试时间数据设置
                fsnmJoinUnionType.setOtherTimestamp(System.currentTimeMillis());

                return fsnmJoinUnionType;
            }
        }).returns(new TypeHint<NMJoinUnionType<F, S>>() {
            @Override
            public TypeInformation<NMJoinUnionType<F, S>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).setParallelism(CommonTestDataGenerateParameters.KAFKA_R_TOPIC_PARTITIONS_NUM);    //TODO 并行度与数据源相同;


        //将第二个流包装成NMJoinUnionType<F, S>类型的流
        DataStream<NMJoinUnionType<F, S>> inputStream2 = secondStream.map(new MapFunction<S, NMJoinUnionType<F, S>>() {
            public NMJoinUnionType<F, S> map(S value) throws Exception {
                NMJoinUnionType<F, S> ssnmJoinUnionType = new NMJoinUnionType<F, S>();
                ssnmJoinUnionType.two(value);

                //TODO 测试时间数据设置
                ssnmJoinUnionType.setOtherTimestamp(System.currentTimeMillis());

                return ssnmJoinUnionType;
            }
        }).returns(new TypeHint<NMJoinUnionType<F, S>>() {
            @Override
            public TypeInformation<NMJoinUnionType<F, S>> getTypeInfo() {
                return super.getTypeInfo();
            }
        }).setParallelism(CommonTestDataGenerateParameters.KAFKA_S_TOPIC_PARTITIONS_NUM);    //TODO 并行度与数据源相同;


        /**************************************************合并两个输入流为一个流******************************************************/
        DataStream<NMJoinUnionType<F, S>> unionStream = inputStream1.union(inputStream2);


        /*************************************************流分区及流复制（Router）*****************************************************/
        DataStream<NMJoinUnionType<F, S>> resultStream = unionStream.process(new NMJoinRouter<F, S>(R_TimeWindows,S_TimeWindows,r_surpass_S,r_behind_S,keySelector_R,keySelector_S))
                .setParallelism(NMJoinConfigParameters.NUM_OF_ROUTER)                           //设置Router并行度
                .flatMap(new NMJoinToGroupRandomFlatMap<F,S>())                                 //将NMJoin转换为分组随机
                .setParallelism(NMJoinConfigParameters.NUM_OF_ROUTER)                           //此转换的并行度与Router相同
                .partitionCustom(new Partitioner<Integer>() {                                   //自定义分区器
                    public int partition(Integer key, int numPartitions) {
                        return key;
                    }
                }, new KeySelector<NMJoinUnionType<F, S>, Integer>() {
                    public Integer getKey(NMJoinUnionType<F, S> value) throws Exception {
                        return value.getNumPartition();
                    }
                })
                .process(new NMJoinJoiner<F, S>(R_TimeWindows,S_TimeWindows,keySelector_R,keySelector_S,r_surpass_S,r_behind_S))     //调用Joiner
                .setParallelism(NMJoinConfigParameters.JOINER_TOTAL_NUM);      //设置Joiner并行度


        //返回结果流
        return resultStream;


    }
}
