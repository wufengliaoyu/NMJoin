package edu.hit.ftcl.wqh.common.fortest;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 用于测试的不产生任何输出的Sink，防止产生结果太多无法处理
 * @param <IN> 输入到Sink中的元组数据类型
 */
public class CommonTestNullResultSinkFunction<IN> extends RichSinkFunction<IN> {
    @Override
    public void invoke(IN value, Context context) throws Exception {
        // 什么也不做
    }
}
