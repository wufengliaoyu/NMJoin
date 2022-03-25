package edu.hit.ftcl.wqh.common.fortest.forTpcH;

public class TpcHParameters {
    //要进行测试的TPC-H测试集中的最小ship date，用于日期到天的转换，用于提取日期键值
    public static final String MIN_SHIP_DATE = "1992-05-01";//"1992-01-02";
    //最大日期对应的天数
    public static final int MAX_DAYS = 2281;//2527;

    //采用supplier作为键值时，supplier的最大值
    public static final int MAX_SUPPLIER = 400000;

}
