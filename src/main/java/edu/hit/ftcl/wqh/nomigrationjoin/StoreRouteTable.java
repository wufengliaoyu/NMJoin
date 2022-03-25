package edu.hit.ftcl.wqh.nomigrationjoin;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class StoreRouteTable {
    //用于保存存储路由表，索引值代表范围连接的键值，对应位置上保存的值代表将要被分配到的分区值
    private ArrayList<Integer> storeRouteTable;

    //序列化器
    private  MySerializerForZookeeperTranslate<ArrayList<Integer>> mySerializer = new MySerializerForZookeeperTranslate<ArrayList<Integer>>();

    //用于存储的joiner的数量
    private int numOfStoreJoiner;
    //要进行范围hash的范围数量
    private int numOfKey;

    //存储所有键值的最小值及最大值
    private double minKey;
    private double maxKey;

    //存储每一个范围hash所包含的范围大小
    private double keyHashRange;


    /**
     * 有参构造器
     */
    public StoreRouteTable(int numOfStoreJoiner, int numOfKey, double minKey, double maxKey) {
        this.numOfStoreJoiner = numOfStoreJoiner;
        this.numOfKey = numOfKey;
        this.minKey = minKey;
        this.maxKey = maxKey;

        //初始化每一个范围hash所包含的范围大小
        keyHashRange = (maxKey - minKey)/numOfKey;

        //用键值的数量来初始化存储路由表
        storeRouteTable = new ArrayList<Integer>(numOfKey);
    }

    public int getNumOfStoreJoiner() {
        return numOfStoreJoiner;
    }

    public int getNumOfKey() {
        return numOfKey;
    }

    public double getMinKey() {
        return minKey;
    }

    public double getMaxKey() {
        return maxKey;
    }

    public double getKeyHashRange() {
        return keyHashRange;
    }

    public ArrayList<Integer> getStoreRouteTable() {
        return storeRouteTable;
    }

    /**
     * 当没有任何前置信息时，从零开始构建一个路由表,一般只应该由调整分区的线程在开始时仅调用一次
     */
    public void init(){

        //大致平均的将所有key分配到所有的joiner中
        int numOfStoreJoinerNow = numOfStoreJoiner;
        int numOfKeyNow = numOfKey;

        int index = 0;
        int partition = 0;

        //当key的数量不能整除joiner数量时，先将多余的key逐渐分配给前几个节点
        while (numOfStoreJoinerNow!=0 && numOfKeyNow % numOfStoreJoinerNow != 0){
            int range = numOfKeyNow / numOfStoreJoinerNow + 1;
            for(int i = 0; i < range ; i++){
                storeRouteTable.add(partition);
                index++;
            }
            partition++;
            numOfKeyNow -= range;
            numOfStoreJoinerNow -= 1;
        }

        if (numOfStoreJoinerNow==0){
            return;
        }

        int range =  numOfKeyNow / numOfStoreJoinerNow;

        //当余数被分配到前几个节点之后，后面的若干个节点肯定能平均分配
        while (numOfStoreJoinerNow!=0){

            for(int i = 0; i < range ; i++){
                storeRouteTable.add(partition);
                index++;
            }
            partition++;
            numOfKeyNow -= range;
            numOfStoreJoinerNow -= 1;
        }

    }

//    /**
//     * 将路由表转换成字符串返回，以方便将路由表上传到zookeeper中
//     * @return 用于保存路由表信息的字符串
//     */
//    public String translateToString(){
//        StringBuilder resultString = new StringBuilder();
//
//        for (RouteEntry re:
//             storeRouteTable) {
//            resultString.append(re.getPartition());
//            resultString.append(" ");
//            resultString.append(re.getTimestamp());
//            resultString.append(";");
//        }
//
//        return resultString.toString();
//    }

    /**
     * 将存储路由表序列化为字节数组
     */
    public byte[] translateToBytes(){
        return mySerializer.serialize(storeRouteTable);
    }

//    /**
//     *  清空原有的路由表，之后利用一个字符串构建新的路由表
//     * @param routeTableString 利用 translateToString 返回的用于构建路由表的字符串
//     */
//    public void parseFromString(String routeTableString){
//        //清除原来的路由表
//        storeRouteTable.clear();
//
//        //解析字符串，构建新的路由表
//        String[] fragment = routeTableString.split(";");
//        int i = 0;
//        for (String s : fragment){
//            String[] se = s.split(" ");
//            storeRouteTable.add(new RouteEntry(i,Integer.parseInt(se[0]),Long.parseLong(se[1])));
//            i++;
//        }
//    }

    /**
     * 清空原有的存储路由表，利用传入的子节数组构建新的存储路由表
     */
    public void parseFromBytes(byte[] input) {
        storeRouteTable.clear();
        storeRouteTable = mySerializer.deserialize(input);
    }

    /**
     * 根据给出的key值，返回当前key的分区，并且在存储路由表中更新对应项的时间戳，使得时间戳一直为该key发往该joiner的最大时间戳
     * @param key 要查找的键值
     * @return 当前要查找键值的分区,若输出的键值不在正确的分区内，则返回-1
     */
    public int getPartitionNum(double key ){

        if (key < minKey || key > maxKey){
            return -1;
        }

        //该键值对应的范围分区的索引位置
        int index;
        index = (int)((key - minKey)/keyHashRange);
        if (index > numOfKey-1){
            index = numOfKey-1;
        }

        //返回找到的分区项
        return storeRouteTable.get(index);
    }


    /**
     * 获取分配给指定分区的最小和最大键值（由于键值循环分布，头尾相接，因此这里最小指的是分配给指定分区的范围的左端点，最大同理指右端点），用于计算负载调度
     * @param partition 要查询最小和最大键值的分区
     * @return 指定分区现在被分配的最小和最大键值所构成的元组 以（leftKey, rightKey）的形式(由于路由表的循环结构，因此 leftKey 可能大于 rightKey )
     *          若指定分区没有被分配键值，则返回 null
     *          若指定分区被分配了所有的键值，则返回（-1，-1）
     */
    public Tuple2<Integer, Integer> getMinKeyAndMaxKeyOfThePartitionNum(int partition) {
        //首先进行正确性判断
        //获取指定分区在路由表中被分配的键值的数量
        int keyNumOfSpecifyPartition = getKeyNumOfSpecifyPartition(partition);
        //若指定分区没有被分配键值，则返回 null
        if (keyNumOfSpecifyPartition <= 0) {
            return null;
        }
        //若指定分区被分配了所有的键值，则返回（-1，-1）
        if (keyNumOfSpecifyPartition >= storeRouteTable.size()) {
            return new Tuple2<Integer, Integer>(-1, -1);
        }

        //找到分配给指定分区的最小（最左）的键值
        //找到分配给该分区的最小的键值的索引
        int index = 0;
        //如果第一项键值就是属于要迁移的分区，则跳过前面的若干个键值
        if (storeRouteTable.get(index)==partition){
            for (int i = 0;i<storeRouteTable.size();i++){
                index++;
                if (storeRouteTable.get(index)!=partition){
                    break;
                }
            }
        }

        //找到属于指定分区的最左侧的键值索引
        //若当前存储路由表中没有给对应分区分配的项，则会出现死循环
        while (storeRouteTable.get(index) != partition) {
            index++;
            index = index % storeRouteTable.size();
        }

        //当前索引即是指向分配给指定分区的最左侧的键值索引
        int leftKey = index;

        //找到属于指定分区的最右侧的键值索引
        while (storeRouteTable.get(index) == partition) {
            index++;
            index = index % storeRouteTable.size();
        }

        if (index == 0) { //若分配给指定分区的键值最右值为整个存储表的右边界，则index此时为0，需要将其置为右边界的值
            index = storeRouteTable.size()-1;
        }else { //当前索引减一即指向分配给指定分区的最右侧的键值索引
            index--;
        }
        int rightKey = index;

        return new Tuple2<Integer, Integer>(leftKey, rightKey);

    }

    /**
     * 获取所有分区所包含的键值范围，用列表的形式返回，在计算负载调度的时候该方法会被调用
     * @return 所有分区包含键值范围的列表，其中每一项的格式是 （分区，最左键值，最右键值）
     */
    public List<Tuple3<Integer, Integer, Integer>> getKeyRangeOfEachPartition() {
        //构建结果列表，其中包含每一个分区中所包含的键值范围
        List<Tuple3<Integer, Integer, Integer>> resultList = new ArrayList<Tuple3<Integer, Integer, Integer>>(numOfStoreJoiner);

        //初始化结果列表
        for (int i = 0; i < numOfStoreJoiner; i++) {
            resultList.add(new Tuple3<Integer, Integer, Integer>(i, null, null));
        }

        //遍历路由表中的每一项，更新结果列表中的每一项的范围
        for (int index = 0; index < storeRouteTable.size(); index++) {
            //获取 index 索引项对应存储路由表中的分区
            int partition = storeRouteTable.get(index);
            //获取 index 被分配给的分区在结果列表中对应的键值范围项
            Tuple3<Integer, Integer, Integer> resultEntry = resultList.get(partition);

            //如果该分区项未被初始化，则初始化对应分区项
            if (resultEntry.f2 == null) {
                resultEntry.f2 = index;
                resultEntry.f1 = index;
            } else {  //如果该分区项已经初始化
                //若新键值索引紧跟着右边界，则扩充右边界
                if (index - resultEntry.f2 == 1) {
                    resultEntry.f2 = index;
                } else { //若键值索引与右边界间隔几个，则说明该分区在存储路由表中跨越了列表尾，已经循环了，则此时左边界为当前索引，右边界不变
                    resultEntry.f1 = index;

                    //此时说明剩余的所有键值项均属于该分区，故而不用再遍历了
                    break;
                }
            }
        }

        //返回结果列表
        return resultList;
    }

    /**
     * 由于指定的分区目前的负载太高，因此将指定分区的若干靠前的键值分配给其之前的分区（按照键值递减，分区值递减的顺序），更新完的最大时间戳为0
     * TODO 若当前存储路由表中没有给对应分区分配的项，则会出现死循环，要保证调用该函数的时候指定分区键值数量不能为0
     * @param partition 负载过高的分区，将该分区的若干最小的键值迁移给分区号比其小一的分区
     * @param rearwardKeyNum 将要迁移的分区数量
     */
    public void forwardSpecifyJoinerSpecifyNumKey(int partition,int  rearwardKeyNum){
        //找到分配给该分区的最小的键值的索引
        int index = 0;
        //如果第一项键值就是属于要迁移的分区，则跳过前面的若干个键值
        if (storeRouteTable.get(index)==partition){
            for (int i = 0;i<storeRouteTable.size();i++){
                index++;
                if (storeRouteTable.get(index)!=partition){
                    break;
                }
            }
        }

        //找到键值分区属于指定分区的最小的键值索引
        //若当前存储路由表中没有给对应分区分配的项，则会出现死循环
        while (true){
            if (storeRouteTable.get(index)==partition){
                break;
            }
            index++;
            index = index % storeRouteTable.size();
        }

        //将要接受迁移键值的分区
        int newPartition = partition - 1;
        if (newPartition < 0){
            newPartition = numOfStoreJoiner - 1;
        }

        //改变rearwardKeyNum个键值的分区
        for (int i = 0;i<rearwardKeyNum;i++){
            //如果当前指定的分区的所有键值都已经被迁移完，则直接跳出
            if (storeRouteTable.get(index)!=partition){
                break;
            }

            //将原本分配给指定分区的键值的分区改变为指定分区号减一
//            storeRouteTable.get(index).setPartition(newPartition);
//            //更新最大的时间戳为watermark
//            storeRouteTable.get(index).setTimestamp(watermark);
            storeRouteTable.set(index, newPartition);

            //循环遍历索引
            index++;
            index = index % storeRouteTable.size();
        }


    }

    /**
     * 由于指定的分区目前的负载太高，因此将指定分区的若干靠后的键值分配给其之后的分区（按照键值递增，分区值递增的顺序），更新完的最大时间戳为0
     * @param partition 负载过高的分区，将该分区的若干最大的键值迁移给分区号比其大一的分区
     * @param backwardKeyNum 将要迁移的分区数量
     * @param watermark 当前同步的所有Router的水位线，当一个元组被调整到一个新的分区时，其最大时间戳即为当前的水位线(目前该参数没有什么用，置为0即可)
     */
    public void backwardSpecifyJoinerSpecifyNumKey(int partition,int  backwardKeyNum,long watermark){
        //找到分配给该分区的最大的键值的索引
        int index = 0;

        //从前到后遍历路由表，找寻一个路由分区为指定分区的项
        for (int i = 0 ;i < storeRouteTable.size();i++){
            //index对应的项分区为指定分区，其键值为该分区的第一个键值
            if (storeRouteTable.get(index)==partition){
                break;
            }
            index++;
        }

        //遍历分配给指定分区的所有项，找寻第一个对应分区不是给指定分区的项
        for (int i = 0 ;i < storeRouteTable.size();i++){
            //如果找到对应的不是分配给指定分区的键值项的第一个项，则跳出，此时index-1为分配给执行分区的最后一个键值项
            if (storeRouteTable.get(index)!=partition){
                index--;
                if (index<0){
                    index = storeRouteTable.size() - 1;
                }
                break;
            }
            index++;
            //如果下一个要访问的路由项越过了数组范围，则重新从数组的第一个开始找
            if (index>=storeRouteTable.size()){
                index=0;
            }
        }

        //将要接受迁移键值的分区(即大于该分区的下一个分区)
        int newPartition = partition + 1;
        if (newPartition >= numOfStoreJoiner){
            newPartition = 0;
        }

        //将分配给指定分区的backwardKeyNum个路由项分配给下一个分区
        for (int i = 0;i< backwardKeyNum;i++){
            if (storeRouteTable.get(index)==partition){

//                //将原本分配给指定分区的键值的分区改变为指定分区号加一
//                storeRouteTable.get(index).setPartition(newPartition);
//                //更新最大的时间戳为watermark
//                storeRouteTable.get(index).setTimestamp(watermark);
                storeRouteTable.set(index, newPartition);

                index--;

                //如果已经搜索到第一个路由项，则跳到最后一个键值路由项，以此实现环形的循环
                if (index<0){
                    index = storeRouteTable.size() - 1;
                }
            }
        }



    }



//    /**
//     * 此方法应该仅由调整路由表线程调用，更新远程路由表的结构（即指定键值的路由分区，而不是简单的时间戳）
//     * @param remoteRouteTable 从zookeeper上获取的保存了远程路由表字符串（已经由各个Router同步过）
//     * @return 改变了结构后的路由表字符串
//     */
//    public String alterRemoteTableStructure(String remoteRouteTable){
//        //存储结果
//        StringBuilder resultString = new StringBuilder();
//        String[]  remoteEntry = remoteRouteTable.split(";");
//
//        for (int i = 0;i<storeRouteTable.size();i++){
//            //远程存储的路由表的第i项，以（分区，时间戳）形式存储
//            String[] rs = remoteEntry[i].split(" ");
//            //当前路由表中的第i项
//            RouteEntry routeEntry = storeRouteTable.get(i);
//
//            //如果远程存储的该项的分区与当前新构建的路由表的分区不同，则以当前新构建的路由表为准
//            if (Integer.parseInt(rs[0])!=routeEntry.getPartition()){
//                //添加新项
//                resultString.append(routeEntry.getPartition());
//                resultString.append(" ");
//                resultString.append(routeEntry.getTimestamp());
//                resultString.append(";");
//
//            }else {//否则以远程路由表为准
//                //添加新项
//                resultString.append(rs[0]);  //分区
//                resultString.append(" ");
//                resultString.append(rs[1]);  //时间戳
//                resultString.append(";");
//            }
//        }
//
//        return resultString.toString();
//    }

    /**
     * 此方法应该仅由调整路由表线程调用，更新远程路由表的结构（即指定键值的路由分区，而不是简单的时间戳）
     *  此处直接将本地的新构建的存储路由表上传到远程，无需获取远程zookeeper上的存储路由表
     * @return 用于更新远程路由表的字节数组
     */
    public byte[] alterRemoteTableStructure() {
        return translateToBytes();
    }

    /**
     * 返回在当前的路由表中，目前为指定分区分配的键值数量
     * @param partition 要查询所包含键值数量的分区
     * @return 指定分区在当前路由表中包含的键值数量
     */
    public int getKeyNumOfSpecifyPartition(int partition){
        int num = 0;
        for (int i = 0;i<storeRouteTable.size();i++){
            if ((storeRouteTable.get(i))==partition){
                num++;
            }
        }
        return num;
    }




    //用于测试的输出路由表的方法
    public String soutRouteTable(){
        StringBuilder stringBuilder = new StringBuilder();
        int i = 0;
        for (Integer e :
                storeRouteTable) {
           stringBuilder.append("<"+i+"-"+e+">");
           stringBuilder.append(":");
           i++;
        }
        return stringBuilder.toString();
    }



}

