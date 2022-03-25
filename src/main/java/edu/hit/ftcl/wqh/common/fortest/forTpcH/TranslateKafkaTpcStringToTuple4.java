package edu.hit.ftcl.wqh.common.fortest.forTpcH;

import edu.hit.ftcl.wqh.tools.CurrentTimeMillisClock;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * 采用 Supplier 作为键值
 */
public class TranslateKafkaTpcStringToTuple4 implements MapFunction<String, Tuple4<String, String, String, String>> {

    private String sourceName;

    private String[] splitStringArr;

    public TranslateKafkaTpcStringToTuple4(String sourceName) {
        this.sourceName = sourceName;
    }

    public TranslateKafkaTpcStringToTuple4() {
    }

    //将字符串转换成之后能够处理的元组形式，元组中所有元素的含义如下：
    //    < 数据源编号【可以为Null，一般无用】-- 键值 -- 时间戳 -- 负载【即原本的整个字符串】>
    @Override
    public Tuple4<String, String, String, String> map(String value) throws Exception {
        splitStringArr = value.split("\\|");
        return new Tuple4<String, String, String, String>(sourceName, splitStringArr[2], "" + System.currentTimeMillis(), value);
    }
}
