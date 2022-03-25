package edu.hit.ftcl.wqh.nomigrationjoin;

import edu.hit.ftcl.wqh.BPlusTreeTest.MySubBPlusTree;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NMJoinJoiner<F,S> extends ProcessFunction<NMJoinUnionType<F,S>,NMJoinUnionType<F,S>> implements CheckpointedFunction {

    //R与S中元组保存的时间(即时间窗口大小)
    private Time R_TimeWindows;
    private Time S_TimeWindows;

    //两个数据流的键值提取器，键值为Double类型
    private KeySelector<F,Double> keySelector_R;
    private KeySelector<S,Double> keySelector_S;

    //用于范围连接，即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
    private double R_surpass_S;
    private double R_behind_S;

    //Joiner中的zookeeper
    private ZooKeeper zkClient;

    //线程同步锁相关，用于在上传本地负载的线程和处理连接的线程之间进行同步
    private Lock uploadLock = new ReentrantLock();
    private Condition uploadLockCondition = uploadLock.newCondition();

    //当前子任务的编号
    private int subTaskIdx;
//    //判断当前任务存储的元组的类型；True：存储R元组，是前面的几个节点之一；False：存储S元组，是后面的几个节点之一；
//    private boolean is_R_StoreTask;

    //表记当前是否为一个周期的结束时刻，用于触发周期变动过程,为True时进行周期的变更，包括新建子树，旧子树过期以及上个周期的负载信息清零
    private volatile boolean isChangeTime = false;

    //用于保存当前的水印
    private Long currentWatermark = 0L;

    //保存上一次重构B+子树的时刻，用于采用事件时间作为子树重构周期的度量标准
    private long lastReconstructBPlusTreeTime = 0L;


    //保存所有B+子树的集合(内部是若干用于保存数据的B+树，树中保存着具体的数据以及对应的键值)
    //R与S各有一棵存储树
    private LinkedList<MySubBPlusTree<NMJoinUnionType<F,S>,Double>> R_Store_SubBPlusTreeSet;
    private LinkedList<MySubBPlusTree<NMJoinUnionType<F,S>,Double>> S_Store_SubBPlusTreeSet;

    //记录当前节点的负载，用一个键值对表示，其中键为对应的元组键值范围，值为对应的负载
    private HashMap<Integer,Double> workloadSet;
    //存储每一个范围hash所包含的范围大小(用于计算到达元组所属的键值分区)
    private double R_KeyHashRange;
    private double S_KeyHashRange;

    //TODO 测试相关
    private long inputTime=0L;

    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(NMJoinJoiner.class.getName());



    /**
     * 重写的初始化方法
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //获取当前节点编号
        subTaskIdx = getRuntimeContext().getIndexOfThisSubtask();
//        //判断当前的任务节点是否是用于存储R流中元组的任务
//        if (subTaskIdx < NMJoinConfigParameters.R_NUM_OF_TASK) {
//            is_R_StoreTask = true;
//        } else {
//            is_R_StoreTask = false;
//        }

        // （1） 初始化zookeeper
        try {
            zkClient = new ZooKeeper(NMJoinConfigParameters.CONNECT_STRING,NMJoinConfigParameters.SESSION_TIMEOUT,null);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：NMJoinJoiner 中 zookeeper 初始化连接失败\n");
        }

        //创建用于保存当前节点负载的zookeeper上的对应节点
        Stat exists = zkClient.exists(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + subTaskIdx, false);
        if (exists == null){
            zkClient.create(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + subTaskIdx,(""+0).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }


        // （2） 初始化B+树的集合(即新建一个子树进行插入)
        if (R_Store_SubBPlusTreeSet == null) {
            MySubBPlusTree<NMJoinUnionType<F, S>, Double> R_NewBPlusSubTree = new MySubBPlusTree<NMJoinUnionType<F, S>, Double>();
            R_Store_SubBPlusTreeSet = new LinkedList<MySubBPlusTree<NMJoinUnionType<F, S>, Double>>();
            R_Store_SubBPlusTreeSet.add(R_NewBPlusSubTree);
        }
        if (S_Store_SubBPlusTreeSet == null) {                       //S也创建一棵树
            MySubBPlusTree<NMJoinUnionType<F, S>, Double> S_NewBPlusSubTree = new MySubBPlusTree<NMJoinUnionType<F, S>, Double>();
            S_Store_SubBPlusTreeSet = new LinkedList<MySubBPlusTree<NMJoinUnionType<F, S>, Double>>();
            S_Store_SubBPlusTreeSet.add(S_NewBPlusSubTree);
        }

        // （3） 初始化本地用于保存负载信息的集合,以及用于确定Double类型的到达存储元组键值所属哪个Integer范围的 hash分区范围
        workloadSet = new HashMap<Integer, Double>();
        //用于存储R流元组，则要计算的负载为到达的R元组的数量
        R_KeyHashRange = (NMJoinConfigParameters.R_ROUTE_TABLE_MAX_KEY - NMJoinConfigParameters.R_ROUTE_TABLE_MIN_KEY) / NMJoinConfigParameters.R_ROUTE_TABLE_KEY_NUM;
        //用于存储S流元组，则要计算的负载为到达的S元组的数量
        S_KeyHashRange = (NMJoinConfigParameters.S_ROUTE_TABLE_MAX_KEY - NMJoinConfigParameters.S_ROUTE_TABLE_MIN_KEY) / NMJoinConfigParameters.S_ROUTE_TABLE_KEY_NUM;


        // （4） 监控中央线程是否通过zookeeper同知该节点上传当前负载，如果标记被改变，则进入负载上传过程
        isChangeTime = false;
        zkClient.exists(NMJoinConfigParameters.SYNC_JOINER_UPLOAD_FLAG, new syncJoinerUploadWatcher());

        // （-5） 监控监视器线程是否通知要收集joiner本地的负载,若通知，则在监视器中上传本地负载，并循环监控
        zkClient.exists(NMJoinConfigParameters.MONITOR_NOTICE_JOINER_LOAD_UPDATE_FLAG,new monitorUploadWatcher());


    }

    /**
     * 当主程序退出时，关闭一些其他线程（主要是zookeeper）
     */
    @Override
    public void close() throws Exception {
        super.close();

        //关闭zookeeper
        zkClient.close();
    }

    /**
     * 有参构造器，进行范围连接的相关初始化参数设置
     */
    public NMJoinJoiner(Time r_TimeWindows, Time s_TimeWindows, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S, double r_surpass_S, double r_behind_S) {
        R_TimeWindows = r_TimeWindows;
        S_TimeWindows = s_TimeWindows;
        this.keySelector_R = keySelector_R;
        this.keySelector_S = keySelector_S;
        R_surpass_S = r_surpass_S;
        R_behind_S = r_behind_S;
    }



    /**
     *核心处理方法
     */
    public void processElement(NMJoinUnionType<F, S> value, Context ctx, Collector<NMJoinUnionType<F, S>> out) throws Exception {

        //当进行周期性调整时,该线程阻塞，直到周期性调整线程处理完毕
        // （因为该方法只在新元组到来时才会被执行，因此若在此处处理周期调整过程，会使得与中央协调线程之间的同步失效）
        uploadLock.lock();
        try {
            //当标记表明当前正在进行负载上传时，该线程被阻塞
            while (isChangeTime) {
                uploadLockCondition.await();
            }

            //更新当前水位线（用于随时保持水位线，让在其他线程当中也能访问当前水位线）
            currentWatermark = ctx.timerService().currentWatermark();

            //如果事件时间达到重构周期，则进行B+树结构的重构 TODO 此处为事件时间
            if ((currentWatermark - lastReconstructBPlusTreeTime) > NMJoinConfigParameters.JOINER_CHANGE_LOCAL_SUB_TREE_PERIOD) {
                lastReconstructBPlusTreeTime = currentWatermark;
                periodChangeLocalStructure(currentWatermark);
            }

            //正常进行元组的连接
            normalProcessElementWithoutSync(value, ctx, out);




        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            uploadLock.unlock();
        }

    }

    /**
     * 正常进行元组连接，不考虑负载上传
     *  该方法被提取出来以便于整体流程展示的清晰
     */
    private void normalProcessElementWithoutSync(NMJoinUnionType<F, S> value, Context ctx, Collector<NMJoinUnionType<F, S>> out) throws Exception{
        //正常进行元组的连接
        // （1） 更新本地负载信息
        updateLocalWorkload(value);

        // （2） 判断当前的元组是否需要进行存储，如果需要，则进行存储
        if (value.isStore()) {
            storeCurrentTuple(value);
        }

        // （3） 所有到达的元组均需要进行连接
        joinCurrentTupleWithOtherWindow(value, ctx, out);



        /*

        // （2） 获取当前元组键值
        Double key;
        //根据当前元组所属的流不同，利用不同的键值提取器提取键值
        if (value.isOne()) {
            key = keySelector_R.getKey(value.getFirstType());
        } else {
            key = keySelector_S.getKey(value.getSecondType());
        }

        // （3） 根据存储还是连接操作的种类不同进行区别对待
        if (value.isStore()) {   //如果当前到达的元组通过标记得知是要进行存储的

            //获取当前元组时间戳
            long timestamp = value.getMaxTimestamp();
            //向B+树集中的当前活动子树插入当前元组（子树集中每次都是位置0处的子树为当前活动子树）
            R_Store_SubBPlusTreeSet.get(0).insert(value, key, timestamp);

        } else {    //如果当前到达的元组通过标记得知是要进行连接的
            //获取将要连接的最大和最小时间戳
            long minTimestamp = value.getMinTimestamp();
            long maxTimestamp = value.getMaxTimestamp();

            //获取当前元组将要进行连接的最大和最小键值
            double minKey;
            double maxKey;
            if (value.isOne()) {   //如果到达的元组是R的元组，即该元组要与若干个存储在本地的S的元组进行连接
                minKey = key - R_surpass_S;
                maxKey = key + R_behind_S;
            } else {    //如果到达的元组是S的元组，即该元组要与若干个存储在本地的R的元组进行连接
                minKey = key - R_behind_S;
                maxKey = key + R_surpass_S;
            }

            //用于保存所有满足键值范围以及时间条件的匹配元组列表
            List<NMJoinUnionType<F, S>> resultMatchList = new ArrayList<NMJoinUnionType<F, S>>();
            //遍历子树集中的所有子树，找寻满足时间条件的子树进行连接
            for (MySubBPlusTree<NMJoinUnionType<F,S>,Double> subBPlusTree : R_Store_SubBPlusTreeSet){
                //如果当前子树的最大时间戳也小于要连接的最小时间戳，或者当前子树的最小时间戳大于要连接的最大时间戳，则当前子树不可能产生结果，直接跳过该子树
                if (subBPlusTree.getMaxTimestamp() < minTimestamp || subBPlusTree.getMinTimestamp() > maxTimestamp) {
                    continue;
                }

                //对于要连接的时间范围与当前子树的时间范围有重叠的情况，进行带有时间限制的范围连接
                List<NMJoinUnionType<F, S>> resultSubMatchList = subBPlusTree.findRange(minKey, maxKey, minTimestamp);

                //将当前子树产生的结果列表添加到最终的列表中
                if (resultSubMatchList != null) {
                    resultMatchList.addAll(resultSubMatchList);
                }

            }

            //将结果合并，输出
            resultGenerateAndOutput(value,resultMatchList,out);
            }

         */




    }

    /**
     * 将当前到达的元组存储到其对应的子树中
     * @param value 到达元组
     */
    private void storeCurrentTuple(NMJoinUnionType<F, S> value) throws Exception {

        //获取当前元组键值
        Double key;
        //根据当前元组所属的流不同，利用不同的键值提取器提取键值
        if (value.isOne()) {    //如果到达的是R中的元组，则插入R的树中

            key = keySelector_R.getKey(value.getFirstType());
            //获取当前元组时间戳
            long timestamp = value.getMaxTimestamp();
            //向R的B+树集中的当前活动子树插入当前元组（子树集中每次都是位置0处的子树为当前活动子树）
            R_Store_SubBPlusTreeSet.get(0).insert(value, key, timestamp);

        } else {    //如果到达的是S中的元组，则插入S的树中

            key = keySelector_S.getKey(value.getSecondType());
            //获取当前元组时间戳
            long timestamp = value.getMaxTimestamp();
            //向S的B+树集中的当前活动子树插入当前元组（子树集中每次都是位置0处的子树为当前活动子树）
            S_Store_SubBPlusTreeSet.get(0).insert(value, key, timestamp);

        }

    }


    /**
     * 将当前到达的元组与其相对应的另一个子树中的所有元组进行匹配
     */
    private void joinCurrentTupleWithOtherWindow(NMJoinUnionType<F, S> value, Context ctx, Collector<NMJoinUnionType<F, S>> out) throws Exception {

        // 获取将要连接的最大和最小时间戳
        long minTimestamp;
        long maxTimestamp;

        // 当前元组键值
        Double key;
        // 获取当前元组将要进行连接的最大和最小键值
        double minKey;
        double maxKey;

        // （1）获取最大和最小时间戳，其中，如果到达的是存储元组，则其要进行连接的最大时间戳是无穷
        minTimestamp = value.getMinTimestamp();
        if (value.isStore()) {
            maxTimestamp = Long.MAX_VALUE;
        } else {
            maxTimestamp = value.getMaxTimestamp();
        }

        // 用于保存所有满足键值范围以及时间条件的匹配元组列表
        List<NMJoinUnionType<F, S>> resultMatchList = new ArrayList<NMJoinUnionType<F, S>>();

        // （2）根据到达的元组类型不同，计算不同的元组连接范围，并进行连接
        if (value.isOne()) {   //如果到达的元组是R的元组，即该元组要与若干个存储在本地的S的元组进行连接

            key = keySelector_R.getKey(value.getFirstType());
            minKey = key - R_surpass_S;
            maxKey = key + R_behind_S;

            //遍历S子树集中的所有子树，找寻满足时间条件的子树进行连接
            for (MySubBPlusTree<NMJoinUnionType<F,S>,Double> subBPlusTree : S_Store_SubBPlusTreeSet){
                //如果当前子树的最大时间戳也小于要连接的最小时间戳，或者当前子树的最小时间戳大于要连接的最大时间戳，则当前子树不可能产生结果，直接跳过该子树
                if (subBPlusTree.getMaxTimestamp() < minTimestamp || subBPlusTree.getMinTimestamp() > maxTimestamp) {
                    continue;
                }

                //对于要连接的时间范围与当前子树的时间范围有重叠的情况，进行带有时间限制的范围连接
                List<NMJoinUnionType<F, S>> resultSubMatchList = subBPlusTree.findRange(minKey, maxKey, minTimestamp);

                //将当前子树产生的结果列表添加到最终的列表中
                if (resultSubMatchList != null) {
                    resultMatchList.addAll(resultSubMatchList);
                }

            }

            //将结果合并，输出
            resultGenerateAndOutput(value,resultMatchList,out);


        } else {    //如果到达的元组是S的元组，即该元组要与若干个存储在本地的R的元组进行连接

            key = keySelector_S.getKey(value.getSecondType());
            minKey = key - R_behind_S;
            maxKey = key + R_surpass_S;

            //遍历R子树集中的所有子树，找寻满足时间条件的子树进行连接
            for (MySubBPlusTree<NMJoinUnionType<F,S>,Double> subBPlusTree : R_Store_SubBPlusTreeSet){
                //如果当前子树的最大时间戳也小于要连接的最小时间戳，或者当前子树的最小时间戳大于要连接的最大时间戳，则当前子树不可能产生结果，直接跳过该子树
                if (subBPlusTree.getMaxTimestamp() < minTimestamp || subBPlusTree.getMinTimestamp() > maxTimestamp) {
                    continue;
                }

                //对于要连接的时间范围与当前子树的时间范围有重叠的情况，进行带有时间限制的范围连接
                List<NMJoinUnionType<F, S>> resultSubMatchList = subBPlusTree.findRange(minKey, maxKey, minTimestamp);

                //将当前子树产生的结果列表添加到最终的列表中
                if (resultSubMatchList != null) {
                    resultMatchList.addAll(resultSubMatchList);
                }

            }

            //将结果合并，输出
            resultGenerateAndOutput(value,resultMatchList,out);
        }

    }


    /**
     * 周期性调整本地的一些数据，包括 上传负载信息 清空本地负载信息 与 新建子树
     *      （1） 遍历保存在本地的负载信息，构建【键值 负载；】形式的负载信息字符串
     *      （2） 将负载信息字符串上传到zookeeper上
     *      （3） 执行子树过期操作(由于过期操作方法内部的设计，必须先进行过期操作之后在新建子树插入，以防止删除新插入的空子树)
     *      （4） 新建活动连接子树（新建的子树插入到本地子树集位置为0的地方）
     *      （5） 清空本地负载信息
     *      （6） 循环监听zookeeper负载信息上传标记
     * TODO 目前调整的周期由中央协调线程控制，每当接收到中央协调线程要求进行负载信息上传时便会开启一个新的负载信息记录过程以及新建子树
     *      但是其实可以考虑其余的周期控制方法，比如用本地固定计时器进行周期调整，后期可以考虑不同方案的优劣
     * @param watermark 当前水位线
     */
    private void periodUploadLocalWorkload(Long watermark) throws KeeperException, InterruptedException {
        //获取上个周期内到达本地的所有键值的集合
        Set<Integer> keySet = workloadSet.keySet();
        //构建用于上传本地负载的字符串信息
        StringBuilder resultString = new StringBuilder();


        // （1） 遍历保存在本地的负载信息，构建【键值 负载；】形式的负载信息字符串
        for (Integer key : keySet) {
            resultString.append(key);
            resultString.append(" ");
            resultString.append(workloadSet.get(key));
            resultString.append(";");
        }

        // （2） 将负载信息字符串上传到zookeeper上
        Stat exists = null;
        try {
            exists = zkClient.exists(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + subTaskIdx, false);
            if (exists == null){
                zkClient.create(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + subTaskIdx,resultString.toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }else {
                zkClient.setData(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + subTaskIdx,resultString.toString().getBytes(),exists.getVersion());
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        // （3） 执行子树过期操作(由于过期操作方法内部的设计，必须先进行过期操作之后再新建子树插入，以防止删除新插入的空子树)
//        //       R与S子树均要进行过期
//        expireTimeWindows(watermark);
//
//        // （4） 新建活动连接子树（新建的子树插入到本地子树集位置为0的地方）
//        // R与S均要重建子树
//        MySubBPlusTree<NMJoinUnionType<F, S>, Double> R_NewBPlusSubTree = new MySubBPlusTree<NMJoinUnionType<F, S>, Double>();
//        R_Store_SubBPlusTreeSet.add(0,R_NewBPlusSubTree);
//        MySubBPlusTree<NMJoinUnionType<F, S>, Double> S_NewBPlusSubTree = new MySubBPlusTree<NMJoinUnionType<F, S>, Double>();
//        S_Store_SubBPlusTreeSet.add(0, S_NewBPlusSubTree);

        // （5） 清空本地负载信息
        workloadSet.clear();

        // （6） 循环监听zookeeper负载信息上传标记
        isChangeTime = false;
        zkClient.exists(NMJoinConfigParameters.SYNC_JOINER_UPLOAD_FLAG, new syncJoinerUploadWatcher());

    }

    /**
     * 该方法周期性的被执行，目的是调整本地的子树结构，包括 旧子树的过期 以及 新子树的插入
     * @param watermark 当前水位线，用于执行过期操作
     */
    private void periodChangeLocalStructure(long watermark) {

        logger.info("进入周期性调整本地B+子树过程");

        // （3） 执行子树过期操作(由于过期操作方法内部的设计，必须先进行过期操作之后再新建子树插入，以防止删除新插入的空子树)
        //       R与S子树均要进行过期
        expireTimeWindows(watermark);

        // （4） 新建活动连接子树（新建的子树插入到本地子树集位置为0的地方）
        // R与S均要重建子树
        MySubBPlusTree<NMJoinUnionType<F, S>, Double> R_NewBPlusSubTree = new MySubBPlusTree<NMJoinUnionType<F, S>, Double>();
        R_Store_SubBPlusTreeSet.add(0,R_NewBPlusSubTree);
        MySubBPlusTree<NMJoinUnionType<F, S>, Double> S_NewBPlusSubTree = new MySubBPlusTree<NMJoinUnionType<F, S>, Double>();
        S_Store_SubBPlusTreeSet.add(0, S_NewBPlusSubTree);

    }

    /**
     * 执行窗口过期操作，以B+子树的粒度(会删除不存在元组的子树)
     * 同时对R的存储子树和S的存储子树进行过期操作
     * @param watermark 当前任务的水位线
     */
    public void expireTimeWindows(Long watermark){
        //一些位置标记
        int index = 0;
        int i = 0;
        //当前种类的任务所需要保存的最小的时间戳
        Long R_MinTimestamp;
        Long S_MinTimestamp;

        //获取子树中需要保存的最小的时间戳
        R_MinTimestamp = watermark - R_TimeWindows.toMilliseconds();
        S_MinTimestamp = watermark - S_TimeWindows.toMilliseconds();

        //遍历所有的R子树，删除其中所有最大时间戳小于 minTimestamp 的子树
        while(i< R_Store_SubBPlusTreeSet.size()){
            i++;
            MySubBPlusTree<NMJoinUnionType<F, S>, Double> subBPlusTree = R_Store_SubBPlusTreeSet.get(index);
            //如果存储的最大时间戳小于当前要求的最小时间，则子树可以安全的删除
            if (subBPlusTree.getMaxTimestamp() < R_MinTimestamp){

                logger.info("R子树集合删除了子树-" + i + " ; 其元组数量为：" + subBPlusTree.getLenght() + " ； 其最大时间戳为：" + subBPlusTree.getMaxTimestamp());

                subBPlusTree.clear();
                R_Store_SubBPlusTreeSet.remove(index);
                continue;
            }
            index++;
        }

        //重置位置索引标记
        index = 0;
        i = 0;

        //遍历所有的S子树，删除其中所有最大时间戳小于 minTimestamp 的子树
        while(i< S_Store_SubBPlusTreeSet.size()){
            i++;
            MySubBPlusTree<NMJoinUnionType<F, S>, Double> subBPlusTree = S_Store_SubBPlusTreeSet.get(index);
            //如果存储的最大时间戳小于当前要求的最小时间，则子树可以安全的删除
            if (subBPlusTree.getMaxTimestamp() < S_MinTimestamp){

                logger.info("S子树集合删除了子树-" + i + " ; 其元组数量为：" + subBPlusTree.getLenght() + " ； 其最大时间戳为：" + subBPlusTree.getMaxTimestamp());

                subBPlusTree.clear();
                S_Store_SubBPlusTreeSet.remove(index);
                continue;
            }
            index++;
        }
    }

    /**
     * 每当有新的元组到达时，进行本地的负载信息更新
     * TODO 一个实验性的方法，只是简单的计算一个周期内到达本节点的存储元组的数量，之后可以实施更复杂的负载判断
     * @param value 当前新到达的元组
     */
    private void updateLocalWorkload(NMJoinUnionType<F, S> value) throws Exception {

        if (value.isStore()) {  //如果当前的到达元组是进行存储的，才进行负载计算
            //获取当前元组键值
            Double key;
            //判断当前元组所属的范围hash的键值分区号
            Integer keyRange;

            //根据当前元组所属的流不同，利用不同的键值提取器提取键值
            if (value.isOne()) {    //如果当前元组是R流元组
                key = keySelector_R.getKey(value.getFirstType());
                //获取所属键值分区号
                keyRange = (int)((key - NMJoinConfigParameters.R_ROUTE_TABLE_MIN_KEY)/R_KeyHashRange);
                if (keyRange > NMJoinConfigParameters.R_ROUTE_TABLE_KEY_NUM - 1) {
                    keyRange = NMJoinConfigParameters.R_ROUTE_TABLE_KEY_NUM - 1;
                }
                if (keyRange < 0) {
                    keyRange = 0;
                }
            } else {
                key = keySelector_S.getKey(value.getSecondType());
                //获取所属键值分区号
                keyRange = (int)((key - NMJoinConfigParameters.S_ROUTE_TABLE_MIN_KEY)/S_KeyHashRange);
                if (keyRange > NMJoinConfigParameters.S_ROUTE_TABLE_KEY_NUM - 1) {
                    keyRange = NMJoinConfigParameters.S_ROUTE_TABLE_KEY_NUM - 1;
                }
                if (keyRange < 0) {
                    keyRange = 0;
                }
            }


            //修改本地用于保存负载的数据结构的值
            if (workloadSet.containsKey(keyRange)) {      //如果当前Map中存储了对应键值区的负载，则在原有负载的基础上加一
                //获取负载
                Double currentKeyLoad = workloadSet.get(keyRange);
                //负载加一
                currentKeyLoad += 1;
                //更新负载
                workloadSet.put(keyRange, currentKeyLoad);
            } else {       //如果当前Map中没有对应键值区的负载，将对应的键值区加入，将其负载置为1
                workloadSet.put(keyRange, (double) 1);
            }

        }  // if(value.isStore()) end

    }



    /**
     * 将当前到达的元组与当前节点中存储的所有满足连接的键值范围与时间范围的元组列表进行连接，并输出结果
     * TODO 在此就是简单的将结果合并输出，以后可以用更加复杂的方法，输出元组的时间参数与新到达的元组时间参数相同
     * @param value 当前到达的新元组
     * @param resultMatchList 当前Joiner当中存储的元组列表中满足连接条件及时间范围的元组列表
     * @param out 输出上下文
     */
    private void resultGenerateAndOutput(NMJoinUnionType<F, S> value,List<NMJoinUnionType<F, S>> resultMatchList,Collector<NMJoinUnionType<F, S>> out){

//        //TODO 测试处理时间
//        value.setMaxTimestamp(System.currentTimeMillis() - inputTime);

        for (NMJoinUnionType<F, S> item : resultMatchList) {
            out.collect(value.union(item));
        }
    }

    /**
     * 负载上传标记监视线程，当中央线程通知joiner进行负载上传时被调用
     */
    class syncJoinerUploadWatcher implements Watcher{

        public void process(WatchedEvent event) {
            //利用标记将主线程阻塞
            isChangeTime = true;

            //进行周期性调整过程，包括 上传负载信息 清空本地负载信息 与 新建子树 以及重置主线程阻塞标记
            uploadLock.lock();
            try {
                //该方法内部会将 isChangeTime 标记置为 false，因此在外部不用再将 isChangeTime 标记改变为 false
                periodUploadLocalWorkload(currentWatermark);

                //唤醒主线程
                uploadLockCondition.signalAll();

            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                uploadLock.unlock();
            }
        }
    }


    /**
     * 在监控器要求joiner上传本地负载时被调用，将本地负载上传，格式为 - 子树树 总负载
     *  目前负载定义为本地存储的元组数
     */
    class monitorUploadWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            //存储的子树数(R+S)
            int size = R_Store_SubBPlusTreeSet.size() + S_Store_SubBPlusTreeSet.size();
            //遍历所有子树，计算总元组数(R+S)
            long totalLoad = 0L;
            for (MySubBPlusTree<NMJoinUnionType<F, S>, Double> e : R_Store_SubBPlusTreeSet) {
                totalLoad += e.getLenght();
            }
            for (MySubBPlusTree<NMJoinUnionType<F, S>, Double> s : S_Store_SubBPlusTreeSet) {
                totalLoad += s.getLenght();
            }

            //上传本地负载
            Stat exists = null;
            try {
                exists = zkClient.exists(NMJoinConfigParameters.MONITOR_EACH_JOIN_LOAD + subTaskIdx, false);
                //构建本地负载的字符串
                byte[] localLoad = ("" + size + " " + totalLoad).toString().getBytes();
                if (exists == null){
                    zkClient.create(NMJoinConfigParameters.MONITOR_EACH_JOIN_LOAD + subTaskIdx, localLoad, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                }else {
                    zkClient.setData(NMJoinConfigParameters.MONITOR_EACH_JOIN_LOAD + subTaskIdx, localLoad, exists.getVersion());
                }

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            //循环监控
            try {
                zkClient.exists(NMJoinConfigParameters.MONITOR_NOTICE_JOINER_LOAD_UPDATE_FLAG,new monitorUploadWatcher());
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }



    /**
     * 状态后端的存储方法
     * TODO 目前没写，以后加入状态管理
     */
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    /**
     * 状态后端的初始化方法
     * TODO 目前没写，以后加入状态管理
     */
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }
}
