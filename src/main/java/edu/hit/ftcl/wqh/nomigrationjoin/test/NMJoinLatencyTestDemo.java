package edu.hit.ftcl.wqh.nomigrationjoin.test;

import edu.hit.ftcl.wqh.common.CommonConfigParameters;
import edu.hit.ftcl.wqh.common.MyCommonTestWatermarkPeriodicAssigner;
import edu.hit.ftcl.wqh.common.fortest.*;
import edu.hit.ftcl.wqh.nomigrationjoin.NMJoinConfigParameters;
import edu.hit.ftcl.wqh.nomigrationjoin.NMJoinUnionStream;
import edu.hit.ftcl.wqh.nomigrationjoin.NMJoinUnionType;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;

public class NMJoinLatencyTestDemo {
    public static void main(String[] args) throws Exception {

        //用日期初始化存储结果的文件名
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        String resultFileName = "NMJoin-Latency_" + dateFormat.format(new Date(System.currentTimeMillis())) + ".txt";

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置发送水位线的时间间隔,500ms
        env.getConfig().setAutoWatermarkInterval(500);
        //设置网络传输刷新时间
        env.setBufferTimeout(5);

//        //构建数据源
//        DataStream<Tuple4<String, String, String, String>> firstStream = env.addSource(new TestCorrectResourceDemo()).assignTimestampsAndWatermarks(new MyCommonTestWatermarkPeriodicAssigner());
//        DataStream<Tuple4<String, String, String, String>> secondStream = env.addSource(new TestCorrectResourceDemo()).assignTimestampsAndWatermarks(new MyCommonTestWatermarkPeriodicAssigner());


        //构建数据源
        DataStream<Tuple4<String, String, String, String>> firstStream = CommonKafkaInputStreamGenerate.getFirstInputStream(env)
                .map(new MapStringToTuple4()).setParallelism(CommonTestDataGenerateParameters.KAFKA_R_TOPIC_PARTITIONS_NUM)
                .assignTimestampsAndWatermarks(new MyCommonTestWatermarkPeriodicAssigner());
        DataStream<Tuple4<String, String, String, String>> secondStream = CommonKafkaInputStreamGenerate.getSecondInputStream(env)
                .map(new MapStringToTuple4()).setParallelism(CommonTestDataGenerateParameters.KAFKA_S_TOPIC_PARTITIONS_NUM)
                .assignTimestampsAndWatermarks(new MyCommonTestWatermarkPeriodicAssigner());

//
//        //记录摄入的时间戳，在Tuple4 的最后一个字段
//        SingleOutputStreamOperator<Tuple4<String, String, String, String>> firstIngestStream = firstStream.map(new MapFunction<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>() {
//            @Override
//            public Tuple4<String, String, String, String> map(Tuple4<String, String, String, String> value) throws Exception {
//                return new Tuple4<>(value.f0, value.f1, value.f2, System.currentTimeMillis() + "");
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple4<String, String, String, String>> secondIngestStream = secondStream.map(new MapFunction<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>() {
//            @Override
//            public Tuple4<String, String, String, String> map(Tuple4<String, String, String, String> value) throws Exception {
//                return new Tuple4<>(value.f0, value.f1, value.f2, System.currentTimeMillis() + "");
//            }
//        });


        //构建联合数据流
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
        DataStream<NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>> resultStream = unionStream.bandNMJoinWithTimeWindow(Time.seconds(600), Time.seconds(600), 1, 1);

        //==================直接在当前程序中处理结果、计算平均延迟的结果处理方式==================
        //        但是会导致窗口算子崩溃，可能是由于某些上游算子在倾斜度高时由于收不到数据而导致水位线无法正常传递，
        //        窗口算子不触发窗口计算，导致内存溢出等原因
        //计算延迟
//        resultStream
//                .map(new MapFunction<NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>, Long>() {    //计算每个元组的延迟
//                     @Override
//                     public Long map(NMJoinUnionType<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> value) throws Exception {
//                         return System.currentTimeMillis() - value.getOtherTimestamp();
//                    }
//                })
//                .setParallelism(NMJoinConfigParameters.TOTAL_NUM_OF_TASK)    //需要设置并行度与Joiner数量一致
//                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.milliseconds(1000)))   //平均延迟计算的<事件>时间窗口大小1000（ms）
//                .apply(new ComputeAverageLatencyAllWindowFunction())    //计算平均延迟
//                .map(new MapFunction<Long, String>() { //在结果后面加入时间戳，用于判断所属时间段
//                    @Override
//                    public String map(Long value) throws Exception {
//                        //在结果后面加入时间戳，用于判断所属时间段
//                        return "" + value + "," + System.currentTimeMillis();
//                    }
//                })
//                .writeAsText("/MyTestDataWQH/" + resultFileName).setParallelism(1);//写入到文件当中

        //==================只在当前程序中计算元组延迟，而将得到的结果输送到Kafka中的结果处理方式==================
        //        当前程序不负责计算平均延迟，需要之后另写程序从kafka中读取数据进行进一步处理，因而不会有窗口触发问题

        //设置Kafka Sink的相关参数
        //设置kafka的相关参数
        Properties kafkaSinkProperties = new Properties();
        kafkaSinkProperties.setProperty("bootstrap.servers", CommonConfigParameters.KAFKA_BROKER_LIST);
        //结果处理
        resultStream
                .map(new ComputeResultTupleLatencyMapFunction())         //计算每个结果元组的延迟
                .setParallelism(NMJoinConfigParameters.JOINER_TOTAL_NUM)    //需要设置并行度与Joiner数量一致
                .flatMap(new ParallelAccumulateResultLatencyMapFunction())    //在每个并行实例上分别累计每一秒的延迟
                .setParallelism(NMJoinConfigParameters.JOINER_TOTAL_NUM)    //需要设置并行度与Joiner数量一致
                .addSink(new FlinkKafkaProducer011<String>(CommonConfigParameters.TEST_RESULTS_COLLECT_KAFKA_TOPIC, new SimpleStringSchema(), kafkaSinkProperties, Optional.ofNullable(null)))
                .setParallelism(CommonConfigParameters.KAFKA_RESULTS_COLLECT_TOPIC_PARTITIONS_NUM);    //设置Sink的并行度与Kafka中分区数量一致


        env.execute();

    }

    /**
     * 将从kafka中读取的字符串数据流转化为Tuple类型
     */
    private static class MapStringToTuple4 implements MapFunction<String, Tuple4<String, String, String, String>> {
        @Override
        public Tuple4<String, String, String, String> map(String value) throws Exception {
            String[] split = value.split(",");
            return new Tuple4<>(split[0],split[1],split[2],split[3]);
        }
    }
}
