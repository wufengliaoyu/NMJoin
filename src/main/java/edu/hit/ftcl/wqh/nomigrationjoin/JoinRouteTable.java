package edu.hit.ftcl.wqh.nomigrationjoin;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class JoinRouteTable {

    //用于保存所有键值在所有joiner中的分布信息，即一个键值发往哪几个分区，以及在这些分区中的时间范围
    private ArrayList<JoinRouteEntry> joinRouteTable;

    //用于保存所有键值将要被发往哪几个分区，该表的存在是为了加快查找效率；
    private ArrayList<LinkedList<PartitionEntry>> searchJoinRouteTable = new ArrayList<LinkedList<PartitionEntry>>();

    //用于存储的joiner的数量
    private int numOfStoreJoiner;
    //要进行范围hash的范围数量,即不同键值数量
    private int numOfKey;

    //存储所有键值的最小值及最大值
    private double minKey;
    private double maxKey;

    //存储每一个范围hash所包含的范围大小
    private double keyHashRange;

    //存储范围连接的范围
    private double lowThanTheKey;
    private double moreThanTheKey;

    //存储该连接路由表中，每个目标元组的时间窗口大小（即元组保存时间）
    private long windowsTime;


    /**
     * 有参构造器，根据给出的存储路由表来构建对应的连接路由表
     * @param storeRouteTable 用于构建连接路由表的存储路由表
     * @param lowThanTheKey 范围连接中比要进行连接的键值小多少
     * @param moreThanTheKey 范围连接中比要进行连接的键值大多少
     * @param windowsTime 当前连接路由表中的元组所要保存的元组的时间窗口大小
     */
    public JoinRouteTable(final StoreRouteTable storeRouteTable,double lowThanTheKey,double moreThanTheKey,long windowsTime){
        this.numOfStoreJoiner = storeRouteTable.getNumOfStoreJoiner();
        this.numOfKey = storeRouteTable.getNumOfKey();
        this.minKey = storeRouteTable.getMinKey();
        this.maxKey = storeRouteTable.getMaxKey();

        this.lowThanTheKey = lowThanTheKey;
        this.moreThanTheKey = moreThanTheKey;

        this.windowsTime = windowsTime;

        //计算hash分区范围
        this.keyHashRange = (maxKey - minKey)/numOfKey;

        //初始化连接路由表，向其中加入numOfKey个项
        joinRouteTable = new ArrayList<JoinRouteEntry>(numOfKey);
        //同时初始化查找表，将其中所有值置为null
        for (int i = 0;i < numOfKey;i++){
            joinRouteTable.add(new JoinRouteEntry(i));
            searchJoinRouteTable.add(null);
        }
    }

//    /**
//     * 将当前的活动存储路由表合并到当前的连接路由表中，其中所有项的最小时间戳为当前的水位线，最大时间戳为 Long.max
//     * @param currentStoreRouteTableString 保存当前的活动存储路由表信息的字符串
//     * @param watermark 当前的水位线
//     * @return 判断该操作是否成功
//     */
//    public boolean insertCurrentStoreTable(String currentStoreRouteTableString,long watermark){
//
//        //解析字符串，更新路由表
//        String[] fragment = currentStoreRouteTableString.split(";");
//
//        //当键值数不同时，报警（后期可改为抛异常）
//        if (fragment.length!=joinRouteTable.size()){
//            System.out.println("insertCurrentStoreTable时加入的存储路由表与连接路由表的项数不相等，即键值数不同");
//            return false;
//        }
//
//
//        for (int i = 0;i < fragment.length;i++){
//            //获取存储路由表中该位置对应的分区
//            String[] storeEntry = fragment[i].split(" ");
//            int partition = Integer.parseInt(storeEntry[0]);
//            //获取连接路由表中该位置对应的键值对应的项
//            JoinRouteEntry joinRouteEntry = joinRouteTable.get(i);
//
//            //向连接路由表中加入该新分区
//            joinRouteEntry.insertNewJoinEntry(partition,watermark);
//        }
//
//        //由于连接路由表结构改变，因此清空之前缓存的搜索表
//        clearSearchJoinRouteTable();
//
//        return true;
//    }

    /**
     * 将当前的活动存储路由表合并到当前的连接路由表中，其中所有项的最小时间戳为当前的水位线，最大时间戳为 Long.max
     * @param storeRouteTable 当前的活动存储路由表（即新从zookeeper上获取的存储路由表）
     * @param watermark 当前的水位线
     * @return 判断该操作是否成功
     */
    public boolean insertCurrentStoreTable(final StoreRouteTable storeRouteTable,long watermark) {
        //获取存储路由表
        ArrayList<Integer> storeRouteTableArr = storeRouteTable.getStoreRouteTable();

        for (int i = 0;i < storeRouteTableArr.size();i++){

            //获取存储路由表中该位置对应的分区
            int partition = storeRouteTableArr.get(i);

            //获取连接路由表中该位置对应的键值对应的项
            JoinRouteEntry joinRouteEntry = joinRouteTable.get(i);

            //向连接路由表中加入该新分区
            joinRouteEntry.insertNewJoinEntry(partition,watermark);
        }

        //由于连接路由表结构改变，因此清空之前缓存的搜索表
        clearSearchJoinRouteTable();

        return true;

    }

//    /**
//     * 用上一个调整路由表周期结束时的最终存储路由表更新连接路由表中对应的项，更新其中分区的最大时间戳
//     * 使用时先用updateWithLastStoreTable方法将上一个周期的路由表“关闭”，之后利用insertCurrentStoreTable方法插入新的、当前正在使用的存储路由表
//     *
//     * @param lastStoreRouteTableString 保存上一个调整路由表周期结束时的最终存储路由表的字符串
//     * @return 判断该操作是否成功
//     */
//    public boolean updateWithLastStoreTable(String lastStoreRouteTableString){
//        //解析字符串，更新路由表
//        String[] fragment = lastStoreRouteTableString.split(";");
//        //当键值数不同时，报警（后期可改为抛异常）
//        if (fragment.length!=joinRouteTable.size()){
//            System.out.println("updateWithLastStoreTable时加入的存储路由表与连接路由表的项数不相等，即键值数不同");
//            return false;
//        }
//
//        for (int i = 0;i < fragment.length;i++){
//            //获取存储路由表中该位置对应的分区,以及该分区的最大时间戳
//            String[] storeEntry = fragment[i].split(" ");
//            int partition = Integer.parseInt(storeEntry[0]);
//            long maxTimestamp = Long.parseLong(storeEntry[1]);
//            //获取连接路由表中该位置对应的键值对应的项
//            JoinRouteEntry joinRouteEntry = joinRouteTable.get(i);
//
//            //更新最大时间戳
//            joinRouteEntry.updateOldJoinEntryMaxTimestamp(partition,maxTimestamp);
//        }
//
//        //由于连接路由表结构改变，因此清空之前缓存的搜索表
//        clearSearchJoinRouteTable();
//
//        return true;
//    }

    /**
     * 用上一个调整路由表周期结束时的最终存储路由表更新连接路由表中对应的项，更新其中分区的最大时间戳
     * 使用时先用updateWithLastStoreTable方法将上一个周期的路由表“关闭”，之后利用insertCurrentStoreTable方法插入新的、当前正在使用的存储路由表
     *
     * @param storeRouteTable 保存上一个调整路由表周期结束时的最终存储路由表
     * @param maxTimestamp 直到目前为止，所有Router中已经收到的元组当中的最大时间戳（一般通过当前的水位线加上一个值来确定）
     *                     该值需要通过所有的Router进行同步后才能获得
     * @return 判断该操作是否成功
     */
    public boolean updateWithLastStoreTable(final StoreRouteTable storeRouteTable , long maxTimestamp) {

        //获取存储路由表
        ArrayList<Integer> storeRouteTableArr = storeRouteTable.getStoreRouteTable();

        for (int i = 0; i < storeRouteTableArr.size(); i++) {

            //获取存储路由表中该位置对应的分区
            int partition = storeRouteTableArr.get(i);
            //获取连接路由表中该位置对应的键值对应的项
            JoinRouteEntry joinRouteEntry = joinRouteTable.get(i);

            //更新最大时间戳
            joinRouteEntry.updateOldJoinEntryMaxTimestamp(partition, maxTimestamp);
        }

        //由于连接路由表结构改变，因此清空之前缓存的搜索表
        clearSearchJoinRouteTable();

        return true;
    }

    /**
     * 删除所有键值的分区当中最大的时间戳小于给出的时间戳的项
     * @param lowTimestamp 删除所有最大时间戳小于该值的分区项
     */
    private void removeExpirePartitionEntry(long lowTimestamp){
        for (JoinRouteEntry e : joinRouteTable){
            e.removeExpirePartitionEntry(lowTimestamp);
        }

        //由于连接路由表结构改变，因此清空之前缓存的搜索表
        clearSearchJoinRouteTable();
    }

    /**
     * 给定当前任务的水位线，根据当前算子的时间窗口大小，删除所有不可能再有可能连接结果的分区
     * @param watermark 当前任务的水位线
     */
    public void removeExpirePartitionEntryWithTimeWindow(long watermark){
        removeExpirePartitionEntry(watermark - this.windowsTime);
    }

    /**
     * 根据给出的键值，以及对应的范围连接的范围，返回在该键值需要进行连接的键值范围内，所有在该范围内的目标键值已经被分配到的所有分区以及对应的时间戳
     * 对于查找目标分区来说，对于每个hash范围内的输入键值来说，只在最开始执行一次查找，之后会将结果保存起来，以方便下次直接使用
     * @param key 要进行连接的键值
     * @return 所有该键值将要连接的目标键值所发往的分区以及分区的时间戳（不考虑窗口时间造成的分区删除）
     */
    private List<PartitionEntry> getPartitionList(double key){
        //如果范围错误，则返回null
        if (key < minKey || key > maxKey){
            return null;
        }

        //该键值对应的范围分区的索引位置
        int index;
        index = (int)((key - minKey)/keyHashRange);
        if (index > numOfKey-1){
            index = numOfKey-1;
        }


        //用于存储返回结果
        LinkedList<PartitionEntry> resultList ;

        if (searchJoinRouteTable.get(index)!=null){ //如果对应key在搜索表中有保存的值，则直接返回
            resultList = searchJoinRouteTable.get(index);
        }else{ //如果要查找的key在搜索表中没有值，则新建值

            //新建一个用于保存该元组将要发往的分区的列表
            resultList = new LinkedList<PartitionEntry>();

            //获取与当前key对应的index所要进行连接的索引的最大值与最小值
            int minIndex;
            int maxIndex;
            minIndex = (int) ((index*keyHashRange - lowThanTheKey)/keyHashRange);
            maxIndex = (int)(((index+1)*keyHashRange + moreThanTheKey)/keyHashRange);
            if (minIndex < 0){
                minIndex = 0;
            }
            if (maxIndex > numOfKey-1){
                maxIndex = numOfKey-1;
            }

            //遍历存储有分区信息的连接路由表
            boolean flag;
            for(int i = minIndex;i<=maxIndex;i++){
                //获取对应索引处对应键值的若干分区信息
                LinkedList<PartitionEntry> partitions = joinRouteTable.get(i).getPartitions();
                //对于该键值的每一个分区，判断其是否已经存在于结果列表中，若已存在，则更新时间戳即可；否则将该分区插入
                for (PartitionEntry jp : partitions){
                    flag = false;
                    //在结果列表中寻找匹配结果
                    for (PartitionEntry rp : resultList){
                        //若该分区已经在结果中，则更新时间戳，扩大范围
                        if (rp.getPartition() == jp.getPartition()){
                            if (jp.getMaxTimestamp() > rp.getMaxTimestamp()){
                                rp.setMaxTimestamp(jp.getMaxTimestamp());
                            }
                            if (jp.getMinTimestamp() < rp.getMinTimestamp()){
                                rp.setMinTimestamp(jp.getMinTimestamp());
                            }
                            flag = true;
                            break;
                        }

                    }
                    //flag为false，表明未被修改，即该分区未在结果中
                    if (!flag){
                        //新建一个分区加入到结果列表中
                        PartitionEntry partitionEntry = new PartitionEntry(index, jp.getPartition(), jp.getMaxTimestamp(), jp.getMinTimestamp());
                        resultList.add(partitionEntry);
                    }
                }

            }

            //将新建的结果列表插入到搜索路由表中，以便于之后查找
            searchJoinRouteTable.set(index,resultList);

        }//else,要查找的key在搜索表中没有值，新建值

        return resultList;
    }

    /**
     * 返回给定键值的满足时间要求（即在时间窗口范围内部）的分区列表，用于指定将要发往的分区及对应分区的时间范围
     * @param key 要查找连接分区的键值
     * @param timestamp 该键值的时间戳，用于筛选将要发往的分区
     * @return 该键值在给定时间戳条件下将要发往的分区列表（该分区列表的最小时间戳是由该键值的时间戳以及窗口大小计算得出的）
     *          此处并没有根据该键值的时间戳确定连接的最大时间戳，因为这种情况应该很少发生，并且若想要确定需要获得当前键值所在流的时间窗口大小
     *          而此处并不容易获取 TODO 之后若想要加入该确认机制加强一致性，可在外部额外加入该功能
     */
    public List<PartitionEntry> getPartitionListWithTimeWindow(double key,long timestamp){
        List<PartitionEntry> partitionList = getPartitionList(key);
        LinkedList<PartitionEntry> resultList = new LinkedList<PartitionEntry>();
        for (PartitionEntry p : partitionList){
            //将所有最大时间戳大于给定键值在当前窗口大小下要连接的最小时间节点的分区项加入到结果列表中
            //只有满足上述条件的分区才有可能存在满足条件的连接结果,此时最小的时间范围根据该键值的时间戳以及窗口大小决定
            if (p.getMaxTimestamp() > timestamp - this.windowsTime){
                if (p.getMinTimestamp() > timestamp - this.windowsTime){
                    resultList.add(new PartitionEntry(p));
                }else{
                    resultList.add(new PartitionEntry(p.getKey(),p.getPartition(),p.getMaxTimestamp(),timestamp - this.windowsTime));
                }

            }
        }
        return resultList;
    }

    /**
     * 清空searchJoinRouteTable缓存表，将其中的每一项都置为null，每当路由表的结构内容发生变化时都需要清空之前的记录了转发分区的搜索表
     */
    private void clearSearchJoinRouteTable(){
        for (int i = 0;i<searchJoinRouteTable.size();i++){
            if (searchJoinRouteTable.get(i)!=null){
                searchJoinRouteTable.get(i).clear();
            }
            searchJoinRouteTable.set(i,null);
        }
    }




    //测试用的输出所有连接路由表项的方法
    public String soutJoinTable(){
        StringBuilder stringBuilder = new StringBuilder();
        for (JoinRouteEntry e : joinRouteTable){
            stringBuilder.append("键值-" + e.key + "\t");
            stringBuilder.append(e.soutEntry());
            System.out.println("\r");
        }

        return stringBuilder.toString();
    }



    /**
     * 路由表中用于存储每一个键值对应的项，该项中保存了分配给该键值的若干分区及时间范围
     */
    private static class JoinRouteEntry{
        //键值
        private double key;
        //保存该键值所对应的若干分区项
        private LinkedList<PartitionEntry> partitions = new LinkedList<PartitionEntry>();

        //对给定键值构造一个新的连接路由表项
        public JoinRouteEntry(double key) {
            this.key = key;
        }

        /**
         * 向该键值中添加一个新的分区
         * @param partition 对应的分区号
         * @param maxTimestamp 该分区存储的对应键值的最大时间戳
         * @param minTimestamp 该分区存储的对应键值的最小时间戳
         */
        public void addPartitionEntry( int partition, long maxTimestamp,long minTimestamp){
            partitions.add(new PartitionEntry(key,partition,maxTimestamp,minTimestamp));
        }

        /**
         *  删除该键值对应的所有过期的分区项
         * @param watermark 最大时间戳小于该时间戳的分区项均会被删除
         */
        public void removeExpirePartitionEntry(long watermark){

            int index = 0;
            for (int i = 0;i < partitions.size();i++){
                if (partitions.get(index).getMaxTimestamp() < watermark){

                    partitions.get(index).clear();
                    partitions.remove(index);
                    continue;
                }
                index++;
            }
        }

        /**
         * 将一个新的存储路由表的项加入到连接路由表项中，若连接路由表中已经存在该分区，则仅更新最大时间戳
         * @param partition 新加入的分区
         * @param watermark 当前加入的水位线，即该分区的最小时间戳
         */
        public void insertNewJoinEntry(int partition,long watermark){
            for (PartitionEntry p : partitions){
                if (p.getPartition()==partition){
                    p.setMaxTimestamp(Long.MAX_VALUE);
                    return;
                }
            }
            partitions.add(new PartitionEntry(key,partition,Long.MAX_VALUE,watermark));
        }

        /**
         * 在一个路由表周期结束时，用上一个已经确定了最大时间戳的存储路由表来更新连接路由表项，在对应的分区中保存当前新的最大的时间戳
         * @param partition 需要更新的分区
         * @param maxTimestamp 需要更新分区的最大时间戳
         */
        public void updateOldJoinEntryMaxTimestamp(int partition,long maxTimestamp){
            for (PartitionEntry p : partitions){
                if (p.getPartition() == partition){
                        p.setMaxTimestamp(maxTimestamp);
                }
            }
        }

        /**
         * 自动生成的get方法
         */
        public double getKey() {
            return key;
        }

        /**
         * 自动生成的get方法
         */
        public LinkedList<PartitionEntry> getPartitions() {
            return partitions;
        }




        //测试用，会输出该键值对应的所有分区项,将其信息转换为字符串返回
        public String soutEntry(){
            StringBuilder stringBuilder = new StringBuilder();
            for (PartitionEntry e : partitions){
                stringBuilder.append(e.toString());
                stringBuilder.append("\n\t\t");
            }
            return stringBuilder.toString();
        }


    }

    /**
     * 用于保存连接路由表中对应的其中一个分区的项，包括要发往的分区号以及在该分区对应的最大和最小时间戳
     */
    public static class PartitionEntry{
        //键值
        private double key;
        //键值对应的分区
        private int partition;
        //该分区对应的最大时间戳
        private long maxTimestamp;
        //该分区对应的最小时间戳
        private long minTimestamp;

        public PartitionEntry(double key, int partition, long maxTimestamp, long minTimestamp) {
            this.key = key;
            this.partition = partition;
            this.maxTimestamp = maxTimestamp;
            this.minTimestamp = minTimestamp;
        }

        /**
         * 利用另一个PartitionEntry来构建一个相同的PartitionEntry
         * @param another 另一个要被复制的PartitionEntry
         */
        public PartitionEntry(PartitionEntry another){
            this.key = another.getKey();
            this.partition = another.getPartition();
            this.maxTimestamp = another.getMaxTimestamp();
            this.minTimestamp = another.getMinTimestamp();
        }

        public PartitionEntry() {
        }

        public double getKey() {
            return key;
        }

        public void setKey(double key) {
            this.key = key;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
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

        /**
         * 清除该项
         */
        public void clear(){
            this.key = 0;
            this.maxTimestamp = 0L;
            this.minTimestamp = 0L;
            this.partition = 0;
        }

        @Override
        public String toString() {
            return "PartitionEntry{" +
                    "key=" + key +
                    ", partition=" + partition +
                    ", maxTimestamp=" + maxTimestamp +
                    ", minTimestamp=" + minTimestamp +
                    '}';
        }
    }
}
