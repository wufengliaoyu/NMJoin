package edu.hit.ftcl.wqh.nomigrationjoin;

import java.io.Serializable;

public class NMJoinUnionType<F,S> implements Serializable {

    //用于序列化，后加的，序列化也是后加的
    private static final long serialVersionUID = 444L;

    //保存连接的两个流的元组数据
    private F firstType = null;
    private S secondType = null;
    //该联合元组将要被发往的分区
    private int numPartition = 0;
    //发往分区后要连接的元组时间范围
    //其中maxTimestamp有时也用于保存当前元组的时间戳，而minTimestamp始终保存将要连接的元组的最小时间戳
    private long maxTimestamp;
    private long minTimestamp;
    //标记该元组是要进行存储还是进行连接(true 表示进行存储；false 表示进行连接)
    private boolean mode;

    //用于后期测试性能的数据
    private long otherTimestamp;


    //无参及有参构造器
    public NMJoinUnionType() {
    }

    public NMJoinUnionType(F firstType, S secondType, int numPartition, long maxTimestamp, long minTimestamp, boolean mode, long otherTimestamp) {
        this.firstType = firstType;
        this.secondType = secondType;
        this.numPartition = numPartition;
        this.maxTimestamp = maxTimestamp;
        this.minTimestamp = minTimestamp;
        this.mode = mode;
        this.otherTimestamp = otherTimestamp;
    }

    //自动生成的所有属性的get，set方法


    public F getFirstType() {
        return firstType;
    }

    public void setFirstType(F firstType) {
        this.firstType = firstType;
    }

    public S getSecondType() {
        return secondType;
    }

    public void setSecondType(S secondType) {
        this.secondType = secondType;
    }

    public int getNumPartition() {
        return numPartition;
    }

    public void setNumPartition(int numPartition) {
        this.numPartition = numPartition;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public boolean isMode() {
        return mode;
    }

    public void setMode(boolean mode) {
        this.mode = mode;
    }

    public long getOtherTimestamp() {
        return otherTimestamp;
    }

    public void setOtherTimestamp(long otherTimestamp) {
        this.otherTimestamp = otherTimestamp;
    }

    /**
     * 构建一个只包含第一个类型的联合类型，此时第二个类型为null
     * @param ft 第一个类型的元组
     */
    public void one(F ft){
        firstType = ft;
        secondType = (S) null;
    }

    /**
     * 构建一个只包含第二个类型的联合类型，此时第二个类型为null
     * @param st 第二个类型的元组
     */
    public void two(S st){
        firstType = (F) null;
        secondType = st;
    }

    /**
     * 判断当前的元组是否为由第一个类型的元组构成（此时联合类型中第二个类型为null）
     * @return true：该联合类型为由第一个类型的元组构成
     */
    public boolean isOne(){
        return (secondType == null)&&(firstType!=null);
    }

    /**
     * 判断当前元组是将要进行存储操作还是连接操作
     * @return true ：进行存储操作
     *          false ： 进行连接操作
     */
    public boolean isStore(){
        return mode;
    }

    /**
     * 将该元组的操作模式置为 存储
     */
    public void setStoreMode(){
        mode = true;
    }

    /**
     * 将该元组的操作模式置为 连接
     */
    public void setJoinMode(){
        mode = false;
    }

    /**
     * 将当前元组与另一个联合类型的元组合并，合并后的元组包含这两个联合元组的存储的元组信息，同时返回元组的时间参数与调用该方法的元组的参数相同
     * @param other 要联合的另外一个联合类型元组
     * @return 包含这两个联合元组的存储的元组信息的联合类型元组，若要联合的两个元组的类型相同，则返回null
     */
    public NMJoinUnionType<F,S> union(NMJoinUnionType<F,S> other){
        //如果两个联合类型元组的类型相同，则返回null
        if(isOne()==other.isOne()){
            return null;
        }
        //如果两个联合类型的类型不容，则构建一个新的包含两个联合类型内容的新联合元组返回，此返回元组的所有时间分区参数与调用该方法的元组的参数相同
        if (isOne()){
            return new NMJoinUnionType<F, S>(getFirstType(),other.getSecondType(),getNumPartition(),getMaxTimestamp(),getMinTimestamp(),isMode(),getOtherTimestamp());
        }else{
            return new NMJoinUnionType<F, S>(other.getFirstType(),getSecondType(),getNumPartition(),getMaxTimestamp(),getMinTimestamp(),isMode(),getOtherTimestamp());
        }
    }

    @Override
    public String toString() {
        return "NMJoinUnionType{" +
                "firstType=" + firstType +
                ", secondType=" + secondType +
                ", numPartition=" + numPartition +
                ", maxTimestamp=" + maxTimestamp +
                ", minTimestamp=" + minTimestamp +
                ", mode=" + mode +
                ", otherTimestamp=" + otherTimestamp +
                '}';
    }
}
