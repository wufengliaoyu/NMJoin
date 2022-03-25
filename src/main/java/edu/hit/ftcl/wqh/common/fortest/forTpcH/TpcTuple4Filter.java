package edu.hit.ftcl.wqh.common.fortest.forTpcH;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;

/**
 * 过滤掉输入中的一些数据，在此处把键值为空的元组过滤掉，后续可以根据查询的条件进行过滤
 */
public class TpcTuple4Filter implements FilterFunction<Tuple4<String, String, String, String>> {

    @Override
    public boolean filter(Tuple4<String, String, String, String> value) throws Exception {
        //如果键值为空就过滤
        if (value.f1.isEmpty()) {
            return false;
        } else {
            //filter函数返回True，则保留
            return true;
        }
    }
}
