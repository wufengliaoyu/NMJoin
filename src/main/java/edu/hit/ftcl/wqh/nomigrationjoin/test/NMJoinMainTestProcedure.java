package edu.hit.ftcl.wqh.nomigrationjoin.test;

import edu.hit.ftcl.wqh.nomigrationjoin.NMJoinConfigParameters;
import edu.hit.ftcl.wqh.nomigrationjoin.NMJoinUnionStream;
import edu.hit.ftcl.wqh.nomigrationjoin.NMJoinUnionType;
import edu.hit.ftcl.wqh.testdatasource.TestCorrectResourceDemo;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 整个NMJoin程序的入口类
 */
public class NMJoinMainTestProcedure {

    public static void main(String[] args) throws Exception {

        /**************************************************相关初始化******************************************************/
        //为使用本地 web ui 而设置的环境
//        Configuration conf = new Configuration();
//        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER,true);
//        conf.setInteger(RestOptions.PORT,8050);

        //本地带web ui的执行环境，和下面那行互换 TODO 测试时用
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        //TODO 一般执行环境，和上面那行互换
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置发送水位线的时间间隔,500ms
        env.getConfig().setAutoWatermarkInterval(500);
        //设置网络传输刷新时间
        env.setBufferTimeout(5);




//        /**************************************************构架数据源******************************************************/
//        DataStream<Tuple4<String, String, String, String>> firstDataStream = env.addSource(new MyFirsrResouce())
//                .assignTimestampsAndWatermarks(new MyPeriodicAssigner1());
//        DataStream<Tuple3<String, String, String>> secondDataStream = env.addSource(new MySecondResource())
//                .assignTimestampsAndWatermarks(new MyPeriodicAssigner2());
//
//
//        /**************************************************实际进行连接******************************************************/
//        //构建合并两个流的合并流类
//        NMJoinUnionStream<Tuple4<String, String, String, String>, Tuple3<String, String, String>> myNMJoinUnionStream = new NMJoinUnionStream<Tuple4<String, String, String, String>, Tuple3<String, String, String>>(firstDataStream, secondDataStream
//                , new KeySelector<Tuple4<String, String, String, String>, Double>() {
//            public Double getKey(Tuple4<String, String, String, String> value) throws Exception {
//                return Double.parseDouble(value.f1);
//            }
//        }, new KeySelector<Tuple3<String, String, String>, Double>() {
//            public Double getKey(Tuple3<String, String, String> value) throws Exception {
//                return Double.parseDouble(value.f1);
//            }
//        });
//
//
//        //进行范围连接
//        DataStream<NMJoinUnionType<Tuple4<String, String, String, String>, Tuple3<String, String, String>>> resultNMJoinUnionTypeDataStream = myNMJoinUnionStream.bandNMJoinWithTimeWindow(Time.milliseconds(2000), Time.milliseconds(2000), 1, 1);
//
//        //输出结果
//        //向文件中写入数据
//        resultNMJoinUnionTypeDataStream.writeAsText("D:\\MyTestData");

        //另一套数据源
        //构建数据源
//        DataStream<Tuple4<String, String, String, String>> firstStream = env.addSource(new FirstFlinkZipfResource()).assignTimestampsAndWatermarks(new MyPeriodicAssigner1());
//        DataStream<Tuple4<String, String, String, String>> secondStream = env.addSource(new SecondFlinkZipfResource()).assignTimestampsAndWatermarks(new MyPeriodicAssigner1());
        DataStream<Tuple4<String, String, String, String>> firstStream = env.addSource(new TestCorrectResourceDemo()).assignTimestampsAndWatermarks(new MyPeriodicAssigner1());
        DataStream<Tuple4<String, String, String, String>> secondStream = env.addSource(new TestCorrectResourceDemo()).assignTimestampsAndWatermarks(new MyPeriodicAssigner1());

        //构建合并两个流的合并流类
        NMJoinUnionStream<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> unionStream = new NMJoinUnionStream<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>(firstStream, secondStream,
                new KeySelector<Tuple4<String, String, String, String>, Double>() {
                    @Override
                    public Double getKey(Tuple4<String, String, String, String> value) throws Exception {
                        return Double.parseDouble(value.f1);
                    }
                }, new KeySelector<Tuple4<String, String, String, String>, Double>() {
            @Override
            public Double getKey(Tuple4<String, String, String, String> value) throws Exception {
                return Double.parseDouble(value.f1);
            }
        });

        //进行范围连接
        DataStream<NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>> resultStream = unionStream.bandNMJoinWithTimeWindow(Time.seconds(10), Time.seconds(10), 10, 10);

        //输出结果（计算连接的两个元组之间的时间差）
//        resultStream.map(new MapFunction<NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>, NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>>() {
//            @Override
//            public NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> map(NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> value) throws Exception {
//                return new NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>(value.getFirstType(),value.getSecondType(),value.getNumPartition(),value.getMaxTimestamp(),value.getMinTimestamp(),value.isMode(),
//                        (Long.parseLong(value.getFirstType().f2)-Long.parseLong(value.getSecondType().f2)));
//            }
//        })
////                .writeAsText("D:\\MyTestData");
////                .print();
//                 .writeAsText("/MyTestDataWah/NMJoinTestResult");

        //输出结果，并计算延迟
        resultStream.print();
//                .map(new MapFunction<NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>, NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>>() {
//            @Override
//            public NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> map(NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> value) throws Exception {
//                long currentTime = System.currentTimeMillis();
//                //如果是在前几个分区进行连接的元组，则说明是到达的S流元组与存储的R元组进行连接，此时获取S的时间
//                if (value.getNumPartition() < NMJoinConfigParameters.R_NUM_OF_TASK) {
//                    long startTime = Long.parseLong(value.getSecondType().f2);
//                    value.setOtherTimestamp(currentTime-startTime);
//                }else {
//                    long startTime = Long.parseLong(value.getFirstType().f2);
//                    value.setOtherTimestamp(currentTime-startTime);
//                }
//                return value;
//            }
//        }).setParallelism(NMJoinConfigParameters.TOTAL_NUM_OF_TASK)
//                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(1000)))
//                .apply(new AllWindowFunction<NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>, Long, TimeWindow>() {
//                    @Override
//                    public void apply(TimeWindow window, Iterable<NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>> values, Collector<Long> out) throws Exception {
//                        //计算平均延迟
//                        long sumTime = 0;
//                        long num = 0;
//                        for (NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> e : values) {
//                            num++;
//                            sumTime += e.getOtherTimestamp();
//                        }
//
//                        if (num == 0) {
//                           return;
//                        } else {
//                            //输出平均延迟
//                            out.collect((sumTime/num));
//                        }
//
//                    }
//                })
//                .writeAsText("/MyTestDataWah/NMJoinTestResult").setParallelism(1);

//                .print();



        //无论如何都要执行的步骤
        env.execute();


    }

    /**
     * 周期性水位线分配器1
     */
    static class MyPeriodicAssigner1 implements AssignerWithPeriodicWatermarks<Tuple4<String, String, String, String>> {

        Long bound = NMJoinConfigParameters.WATERMARK_AFTER_MAX_TIMESTAMP;//水位线的延迟，ms
        Long maxTs = Long.MIN_VALUE;//观察到的最大时间戳
        @Nullable
        public Watermark getCurrentWatermark() {
            //防止在来数据之前生成水位线导致溢出
            if (maxTs == Long.MIN_VALUE){
                return new Watermark(maxTs);
            }else {
                return new Watermark(maxTs-bound);
            }
//            return new Watermark(maxTs-bound);
        }

        public long extractTimestamp(Tuple4<String, String, String, String> element, long recordTimestamp) {
            long timestamp = Long.parseLong(element.f2);
            if (timestamp>maxTs){
                maxTs = timestamp;
            }
            return timestamp;
        }
    }

    /**
     * 周期性水位线分配器2
     */
    static class MyPeriodicAssigner2 implements AssignerWithPeriodicWatermarks<Tuple3<String, String, String>>{

        Long bound = 500L;//水位线的延迟，ms
        Long maxTs = Long.MIN_VALUE;//观察到的最大时间戳
        @Nullable
        public Watermark getCurrentWatermark() {
            return new Watermark(maxTs-bound);
        }

        public long extractTimestamp(Tuple3<String, String, String> element, long recordTimestamp) {
            long timestamp = Long.parseLong(element.f2);
            if (timestamp>maxTs){
                maxTs = timestamp;
            }
            return timestamp;
        }
    }

}
