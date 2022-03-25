package edu.hit.ftcl.wqh.common.fortest;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 用于计算时间窗口范围内的平均延迟
 */
public class ComputeAverageLatencyAllWindowFunction implements AllWindowFunction<Long, Long, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<Long> values, Collector<Long> out) throws Exception {
        //计算平均延迟
        long sumTime = 0;
        long num = 0;
        for (Long e : values) {
            num++;
            sumTime += e;
        }

        if (num == 0) {
            out.collect(-1L);
        } else {
            //输出平均延迟
            out.collect((sumTime / num));
        }
    }
}
