package edu.hit.ftcl.wqh.nomigrationjoin;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Router类，用于元组的复制与元组分发
 * 用 ProcessFunction 函数实现，目的是访问时间戳以及水位线
 * @param <F> R流类型
 * @param <S> S流类型
 */
public class NMJoinRouter<F,S> extends ProcessFunction<NMJoinUnionType<F,S>,NMJoinUnionType<F,S>> {

    //Router中的zookeeper
    private ZooKeeper zkClient;

    //标记当前是否正在进行路由表重构，如果是，则需要阻塞对于新元组的处理;初始化为false，即不阻塞
    private boolean isChangeRouteTable = false;
    //线程同步锁相关，用于在监控zookeeper的线程和处理到来元组的线程之间进行同步
    private Lock watcherLock = new ReentrantLock();
    //      用于 更新路由表线程 阻塞 主处理线程 的锁
    private Condition watcherLockCondition = watcherLock.newCondition();
    //      用于 监控中央线程上传路由表完成线程 阻塞 更新路由表线程 的锁
    private Condition watcherUpdateCompleteCondition = watcherLock.newCondition();

    //当前水位线
    private long currentWatermark = 0;

    //在进行路由表更改操作期间，用于标记中央协调线程是否完成新路由表的生成与上传，如果为True，则表示可以从远程获取路由表的最新信息
    private boolean isCentralCoordinatorUpdateComplete = false;



    //保存的R与S流的存储路由表
    private StoreRouteTable Both_StoreRouteTable;
    //保存的R流的连接路由表
    private JoinRouteTable R_JoinRouteTable;
    //保存的S流的连接路由表
//    private StoreRouteTable S_StoreRouteTable;
    private JoinRouteTable S_JoinRouteTable;

    //R与S中元组保存的时间(即时间窗口大小)
    private Time R_TimeWindows;
    private Time S_TimeWindows;

    //用于范围连接，即S - R_behind_S < R < S + R_surpass_S ;R - R_surpass_S < S < R + R_behind_S
    private double R_surpass_S;
    private double R_behind_S;

    //两个数据流的键值提取器，键值为Double类型
    private KeySelector<F,Double> keySelector_R;
    private KeySelector<S,Double> keySelector_S;

    //指标监控
    private Meter myMeter;

    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(NMJoinRouter.class);

    public NMJoinRouter(Time r_TimeWindows, Time s_TimeWindows, double r_surpass_S, double r_behind_S, KeySelector<F, Double> keySelector_R, KeySelector<S, Double> keySelector_S) {
        R_TimeWindows = r_TimeWindows;
        S_TimeWindows = s_TimeWindows;
        R_surpass_S = r_surpass_S;
        R_behind_S = r_behind_S;
        this.keySelector_R = keySelector_R;
        this.keySelector_S = keySelector_S;
    }

    /**
     * 初始化zookeeper及两个存储路由表以及两个连接路由表
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //在flatMap的最后一个并行子任务中启动中央协调线程
        int numberOfParallel = getRuntimeContext().getNumberOfParallelSubtasks();
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        if (indexOfThisSubtask == NMJoinConfigParameters.NUM_OF_ROUTER -1){
            //启动中央协调线程
            NMJoinCentralCoordinator nmJoinCentralCoordinator = new NMJoinCentralCoordinator();
            nmJoinCentralCoordinator.start();
        }

        //指标系统初始化
        MetricGroup my_custom_throughput_monitor = getRuntimeContext()
                .getMetricGroup()
                .addGroup("my_custom_throughput_monitor");
        Counter inputCounter = my_custom_throughput_monitor.counter("InputCounter");
        myMeter = my_custom_throughput_monitor.meter("MyThroughputMeter",new MeterView(inputCounter,30));

        //所有Router线程休眠一段时间，以待中央协调线程启动成功
        Thread.sleep(100);

        //初始化zookeeper
        try {
            zkClient = new ZooKeeper(NMJoinConfigParameters.CONNECT_STRING,NMJoinConfigParameters.SESSION_TIMEOUT,null);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：NMJoinRouter 中 zookeeper 初始化连接失败\n");
        }


        //获取zookeeper上存储的存储路由表信息(字节数组)
        Stat R_exists = zkClient.exists(NMJoinConfigParameters.BOTH_STORE_ROUTE_TABLE_PATH, false);
        while (R_exists == null){
            Thread.sleep(10);
            R_exists = zkClient.exists(NMJoinConfigParameters.BOTH_STORE_ROUTE_TABLE_PATH, false);
        }
        byte[] Both_StoreRouteTableByte = zkClient.getData(NMJoinConfigParameters.BOTH_STORE_ROUTE_TABLE_PATH, false, new Stat());


        //初始化并且构建存储路由表
        Both_StoreRouteTable = new StoreRouteTable(NMJoinConfigParameters.TOTAL_GROUP_NUM_OF_TASK,NMJoinConfigParameters.R_ROUTE_TABLE_KEY_NUM,NMJoinConfigParameters.R_ROUTE_TABLE_MIN_KEY,NMJoinConfigParameters.R_ROUTE_TABLE_MAX_KEY);
        Both_StoreRouteTable.parseFromBytes(Both_StoreRouteTableByte);
        //利用存储路由表构建R的连接路由表
        R_JoinRouteTable = new JoinRouteTable(Both_StoreRouteTable,R_behind_S,R_surpass_S,R_TimeWindows.toMilliseconds());
        R_JoinRouteTable.insertCurrentStoreTable(Both_StoreRouteTable,0);
        //利用存储路由表构建S的连接路由表
        S_JoinRouteTable = new JoinRouteTable(Both_StoreRouteTable,R_surpass_S,R_behind_S,S_TimeWindows.toMilliseconds());
        S_JoinRouteTable.insertCurrentStoreTable(Both_StoreRouteTable,0);


        //开启路由表周期性调整的监视，当中央协调线程发出同步信号时，调用监视器函数，进行同步
        isChangeRouteTable = false;
        zkClient.exists(NMJoinConfigParameters.SYNC_ROUTER_STORE_TABLE_FLAG, new synchronizationWatcher());




        //TODO 测试,输出一下指定分区的连接路由表
//        if (indexOfThisSubtask == numberOfParallel-2){
//            //测试
//            System.out.println("flatmap任务： "+getRuntimeContext().getIndexOfThisSubtask() + " 的 S_JoinRouteTable");
//        S_JoinRouteTable.soutJoinTable();
////            S_StoreRouteTable.soutRouteTable();
//
//            //测试
//            System.out.println("flatmap任务： "+getRuntimeContext().getIndexOfThisSubtask() + " 的 R_JoinRouteTable");
//        R_JoinRouteTable.soutJoinTable();
//
//        R_StoreRouteTable.soutRouteTable();
////            R_StoreRouteTable.soutRouteTable();
//        }


    }


    /**
     * 当主程序退出时，关闭一些多余的线程（主要是zookeeper）
     */
    @Override
    public void close() throws Exception {
        super.close();
        //关闭zookeeper
        zkClient.close();
    }

    /**
     * 实际的处理到来元组的方法，会对同一个元组分别进行存储操作以及连接操作。
     * 对于存储操作，会根据该元组对应的存储路由表，将该元组发送到其对应的存储分区，存储分区只有一个
     * 对于连接操作，会根据该元组对应的连接路由表，将该元组发送到所有对应的连接分区，连接分区可能有多个
     */
    public void processElement(NMJoinUnionType<F, S> value, Context ctx, Collector<NMJoinUnionType<F, S>> out) throws Exception {

        //指标加1，用于计算平均吞吐
        myMeter.markEvent();

            //如果同步标记表示当前正在进行同步，则阻塞新到元组的处理（必须以同步的方法进行处理，否则结果一致性不会被满足）
            watcherLock.lock();
            try {

                //如果标记表明当前正在进行路由表更新，则阻塞在这里
                while (isChangeRouteTable) {
                    watcherLockCondition.await();
                }

                //更新当前水位线
                currentWatermark = ctx.timerService().currentWatermark();

                //正常的进行元组处理
                normalProcessElementWithoutSynchronization(value, ctx, out);

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                watcherLock.unlock();
            }


    }

    /**
     * 正常的进行元组处理过程，不涉及到路由表的更新
     */
    public void normalProcessElementWithoutSynchronization(NMJoinUnionType<F, S> value, Context ctx, Collector<NMJoinUnionType<F, S>> out) throws Exception{
        //正常处理元组

//        //TODO 测试，测试从map传入Router的时间
//        value.setOtherTimestamp(System.currentTimeMillis()-value.getOtherTimestamp());

        //获取当前元组的时间戳
        Long timestamp = ctx.timestamp();
        //对于R的元组与S的元组区别处理
        if (value.isOne()){  //如果是R流的元组
            //（1）获取键值
            Double key = keySelector_R.getKey(value.getFirstType());

            //（2）查询一个存储元组
            //根据键值，查找存储路由表，获取元组存储对应的分区值
            int R_StorePartitionNum = Both_StoreRouteTable.getPartitionNum(key);

            /*
            //创建存储元组
            NMJoinUnionType<F, S> R_StoreTuple = new NMJoinUnionType<F, S>();
            //设置存储元组的值，时间戳（最大最小均设为当前元组的时间戳），分区，存储/连接模式
            R_StoreTuple.one(value.getFirstType());
            R_StoreTuple.setMaxTimestamp(timestamp);
            R_StoreTuple.setMinTimestamp(timestamp);
            R_StoreTuple.setNumPartition(R_StorePartitionNum);
            R_StoreTuple.setOtherTimestamp(value.getOtherTimestamp());  //TODO 设置测试时间戳
            R_StoreTuple.setStoreMode();
            //将存储元组发出
            out.collect(R_StoreTuple);

             */

            //（3）构建多个连接元组
            //查找S的连接路由表，查找要发往的分区以及对应的最大/最小时间戳
            List<JoinRouteTable.PartitionEntry> partitionListWithTimeWindow = S_JoinRouteTable.getPartitionListWithTimeWindow(key, timestamp);
            //对于所有满足时间要求的分区，均发出一个连接元组
            for (JoinRouteTable.PartitionEntry p : partitionListWithTimeWindow){

                //创建连接元组
                NMJoinUnionType<F, S> R_JoinTuple = new NMJoinUnionType<F, S>();
                //设置连接元组的值，要连接的最大时间戳，要连接的最小时间戳，分区（需要加上R节点的数量，因为S的元组存储在后几个节点当中），存储/连接模式
                R_JoinTuple.one(value.getFirstType());
                R_JoinTuple.setMaxTimestamp(p.getMaxTimestamp());
                R_JoinTuple.setMinTimestamp(p.getMinTimestamp());
                R_JoinTuple.setNumPartition(p.getPartition()); //分区数不用加上前面的若干个R的存储节点，因为不区分R与S了
                R_JoinTuple.setOtherTimestamp(value.getOtherTimestamp());       //TODO 设置测试时间戳
                R_JoinTuple.setJoinMode();

                //如果该元组是要存储到对应的joiner中的，则将其模式改为store，并且将最大的时间戳设置为当前元组的时间戳
                // （因为正常情况下其要连接的最大时间戳为无穷，是个固定值，因此在此可以利用该额外字段来存储额外的信息）
                if (p.getPartition() == R_StorePartitionNum) {
                    R_JoinTuple.setMaxTimestamp(timestamp);
                    R_JoinTuple.setStoreMode();
                }

                //将连接元组发出
                out.collect(R_JoinTuple);
            }

        }else {     //如果是S流的元组
            //（1）获取键值
            Double key = keySelector_S.getKey(value.getSecondType());

            //（2）查询一个存储元组
            //根据键值，查找存储路由表，获取元组存储对应的分区值(存储分区需要加上前面的若干个R存储节点的数量)
            int S_StorePartitionNum = Both_StoreRouteTable.getPartitionNum(key);

            /*
            //创建存储元组
            NMJoinUnionType<F, S> S_StoreTuple = new NMJoinUnionType<F, S>();
            //设置存储元组的值，时间戳（最大最小均设为当前元组的时间戳），分区，存储/连接模式
            S_StoreTuple.two(value.getSecondType());
            S_StoreTuple.setMaxTimestamp(timestamp);
            S_StoreTuple.setMinTimestamp(timestamp);
            S_StoreTuple.setNumPartition(S_StorePartitionNum);
            S_StoreTuple.setOtherTimestamp(value.getOtherTimestamp());     //TODO 设置测试时间戳
            S_StoreTuple.setStoreMode();
            //将存储元组发出
            out.collect(S_StoreTuple);

             */

            //（3）构建多个连接元组
            //查找R的连接路由表，查找要发往的分区以及对应的最大/最小时间戳
            List<JoinRouteTable.PartitionEntry> partitionListWithTimeWindow = R_JoinRouteTable.getPartitionListWithTimeWindow(key, timestamp);
            //对于所有满足时间要求的分区，均发出一个连接元组
            for (JoinRouteTable.PartitionEntry p : partitionListWithTimeWindow){

                //创建连接元组
                NMJoinUnionType<F, S> S_JoinTuple = new NMJoinUnionType<F, S>();
                //设置连接元组的值，要连接的最大时间戳，要连接的最小时间戳，分区（需要加上R节点的数量，因为S的元组存储在后几个节点当中），存储/连接模式
                S_JoinTuple.two(value.getSecondType());
                S_JoinTuple.setMaxTimestamp(p.getMaxTimestamp());
                S_JoinTuple.setMinTimestamp(p.getMinTimestamp());
                S_JoinTuple.setNumPartition(p.getPartition()); //分区数不用加额外的东西
                S_JoinTuple.setOtherTimestamp(value.getOtherTimestamp());      //TODO 设置测试时间戳
                S_JoinTuple.setJoinMode();

                //如果该元组是要存储到对应的joiner中的，则将其模式改为store，并且将最大的时间戳设置为当前元组的时间戳
                // （因为正常情况下其要连接的最大时间戳为无穷，是个固定值，因此在此可以利用该额外字段来存储额外的信息）
                if (p.getPartition() == S_StorePartitionNum) {
                    S_JoinTuple.setMaxTimestamp(timestamp);
                    S_JoinTuple.setStoreMode();
                }

                //将连接元组发出
                out.collect(S_JoinTuple);
            }
        }

    }

    /**
     * 周期性进行路由表调整的方法，该方法执行时，主线程会被阻塞
     * @param watermark 当前水位线，用于更新远程zookeeper上的水位线
     */
    private void periodChangeRouteTableProcess(long watermark) throws KeeperException, InterruptedException {
        // --不再需要同步存储路由表了--(1) 同步远程存储路由表
//        syncRemoteRouterStoreTable();

        // (2) 同步水位线
        syncRemoteMinAndMaxWatermark(watermark);

        // (3) 利用zookeeper通知同步存储路由表操作结束，并获取 上一个周期最后完成同步的远程路由表 以及 最新的存储路由表,并更新本地存储路由表与连接路由表(包括连接路由表过期操作）
        noticeCentralAndGetNewStoreRouterTable();

        // (4) 更改本地 路由表改变 标记，表明当前的本地路由表更改完成，接下来可以进行正常到来元组的处理了
        isChangeRouteTable = false;

        // (5) 重新监视同步节点
        zkClient.exists(NMJoinConfigParameters.SYNC_ROUTER_STORE_TABLE_FLAG, new synchronizationWatcher());

    }


    /**
     * 同步zookeeper上的存储路由表
     * 步骤为：（1）获取zookeeper上的路由表
     *          （2）用本地存储路由表更新获取的路由表
     *          （3）将更新后的路由表字符串写到zookeeper上对应的节点中
     *          （4）如果（3）失败，则重复由（1）开始
     */
//    private void syncRemoteRouterStoreTable() throws KeeperException, InterruptedException {
//
//        //更新远程路由表失败标记
//        boolean isFailed = true;
//
//        //重复执行R的存储路由表的 获取->写入 远程存储路由表的操作，直到成功
//        while (isFailed){
//            //定义获取远程zookeeper数据操作的状态
//            Stat R_stat = new Stat();
//            //获取zookeeper上的R同步路由表，并获取该操作的状态
//            byte[] R_StoreTableByte = zkClient.getData(NMJoinConfigParameters.R_STORE_ROUTE_TABLE_PATH, false,R_stat);
//            //获取到的远程路由表字符串
//            String R_StoreTableString = new String(R_StoreTableByte);
//
//            //利用本地存储路由表更新远程存储路由表的最大时间戳
//            String resultRemote_R_StoreTableString = Both_StoreRouteTable.updateRemoteTableWithString(R_StoreTableString);
//
//            //失败标记为假，即当前操作成功
//            isFailed = false;
//
//            //更新远程路由表，如果失败，表明处理过程中有其他节点更改过该节点，故而重新进行获取数据的步骤
//            try {
//                zkClient.setData(NMJoinConfigParameters.R_STORE_ROUTE_TABLE_PATH,resultRemote_R_StoreTableString.getBytes(),R_stat.getVersion());
//            }catch (Exception e){
//                //表明当前操作失败
//                isFailed = true;
//            }
//        }
//
//        //重新置失败标记为真，表明当前操作未成功
//        isFailed = true;
//
//        //重复执行S的存储路由表的 获取->写入 远程存储路由表的操作，直到成功
//        while (isFailed){
//            //定义获取远程zookeeper数据操作的状态
//            Stat S_stat = new Stat();
//            //获取zookeeper上的S同步路由表，并获取该操作的状态
//            byte[] S_StoreTableByte = zkClient.getData(NMJoinConfigParameters.S_STORE_ROUTE_TABLE_PATH, false,S_stat);
//            //获取到的远程路由表字符串
//            String S_StoreTableString = new String(S_StoreTableByte);
//
//            //利用本地存储路由表更新远程存储路由表的最大时间戳
//            String resultRemote_S_StoreTableString = S_StoreRouteTable.updateRemoteTableWithString(S_StoreTableString);
//
//            //失败标记为假，即当前操作成功
//            isFailed = false;
//
//            //更新远程路由表，如果失败，表明处理过程中有其他节点更改过该节点，故而重新进行获取数据的步骤
//            try {
//                zkClient.setData(NMJoinConfigParameters.S_STORE_ROUTE_TABLE_PATH,resultRemote_S_StoreTableString.getBytes(),S_stat.getVersion());
//            }catch (Exception e){
//                //表明当前操作失败
//                isFailed = true;
//            }
//        }
//
//
//
//    }

    /**
     * 更新zookeeper上的最小水位线和最大水位线节点，用当前Router的水位线与zookeeper中保存的水位线作比较，将最小值与最大值放入zookeeper中
     * (远程保存的水位线的值已经利用中央协调线程事先置为long最大值，和Long的最小值)
     * @param watermark 当前水位线
     */
    private void syncRemoteMinAndMaxWatermark(long watermark) throws KeeperException, InterruptedException {

        //失败与否判断标记，初始为真，即尚未成功
        boolean isFailed = true;

        //直到成功之前，一直执行 读取->判断->写入 的过程
        while (isFailed){
            //将zookeeper上的保存最小水位线的节点的值置为Long的最大值，用于更新最小水位线
            Stat Min_Watermark_exists = new Stat();
            Stat Max_Watermark_exists = new Stat();

            //获取zookeeper上的同步数位线(R_SYNC_MIN_WATERMARK_PATH中存储的是最小的水位线，S_SYNC_MIN_WATERMARK_PATH中存储的是最大的水位线)
            byte[] Min_WaterMarkByte = zkClient.getData(NMJoinConfigParameters.SYNC_MIN_WATERMARK_PATH, false, Min_Watermark_exists);
            byte[] Max_WaterMarkByte = zkClient.getData(NMJoinConfigParameters.SYNC_MAX_WATERMARK_PATH, false, Max_Watermark_exists);
            long Min_SyncWatermark = Long.parseLong(new String(Min_WaterMarkByte));
            long Max_SyncWatermark = Long.parseLong(new String(Max_WaterMarkByte));

            //如果远程存储的水位线最小值小于本地的水位线并且最大值大于本地水位线时，直接返回，不做处理
            if (Min_SyncWatermark <= watermark && Max_SyncWatermark >= watermark){
                return;
            }

            //失败标记为假，即到目前为止是成功的
            isFailed = false;

            //向远程写入最小的水位线
            try {

                if (watermark < Min_SyncWatermark){
                    zkClient.setData(NMJoinConfigParameters.SYNC_MIN_WATERMARK_PATH,(""+watermark).getBytes(),Min_Watermark_exists.getVersion());
                }

                if (watermark > Max_SyncWatermark){
                    zkClient.setData(NMJoinConfigParameters.SYNC_MAX_WATERMARK_PATH,(""+watermark).getBytes(),Max_Watermark_exists.getVersion());
                }

            }catch (Exception e){
                //标记本次操作失败
                isFailed = true;
            }
        }


    }

    /**
     * 通知中央线程，当前节点的同步完成，并等待中央线程计算并上传新的存储路由表，然后本节点从远程获取最新的存储路由表、上一个周期的同步路由表以及同步最小水位线
     * 具体功能为：
     *      （1）监视同步节点
     *      （2）更改本节点同步完成标记,表示本节点同步操作已经完成
     *      （3）等待同步节点通知中央线程更新路由表完成
     *
     *      （4）获取上一个周期的同步路由表
     *      （5）获取最新路由表
     *      （6）获取最小水位线
     *
     *      （7）更新本地连接路由表
     *      （8）更新本地存储路由表
     *
     *      （9）对连接路由表执行过期操作
     *
     */
    private void noticeCentralAndGetNewStoreRouterTable() throws KeeperException, InterruptedException {

        //获取当前路由节点的编号
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

        //（1）监控同步标记，当中央线程更改同步标记表示zookeeper上最新路由表更新完成时，本节点从远程获取最新的信息
        isCentralCoordinatorUpdateComplete = false;
        zkClient.exists(NMJoinConfigParameters.SYNC_ROUTER_STORE_TABLE_FLAG,new centralCoordinatorUpdateWatcher());

        //（2）更改远程zookeeper中对应的表示本节点标记完成的节点的值，表示本节点已经完成的zookeeper上存储路由表与最小水位线的同步
        Stat thisRouterSyncCompleteNodeExists = zkClient.exists(NMJoinConfigParameters.EACH_ROUTER_SYNC_COMPLETE_FLAG + indexOfThisSubtask, false);
        if (thisRouterSyncCompleteNodeExists == null){     //如果该路由节点对应的zookeeper上的同步完成节点未创建，则创建；否则直接更改其值
            zkClient.create(NMJoinConfigParameters.EACH_ROUTER_SYNC_COMPLETE_FLAG + indexOfThisSubtask,"RouterSyncComplete".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }else {
            zkClient.setData(NMJoinConfigParameters.EACH_ROUTER_SYNC_COMPLETE_FLAG + indexOfThisSubtask,"RouterSyncComplete".getBytes(),thisRouterSyncCompleteNodeExists.getVersion());
        }


        //（3）监控isCentralCoordinatorUpdateComplete标记是否被中央协调线程改变，若该标记被改变，则表示中央协调线程已经将最新的存储路由表上传完成
//        while (!isCentralCoordinatorUpdateComplete){
//            //若未被改变，则休眠10ms以等待完成
//            Thread.sleep(10);
//        }
        //  利用同步锁的方式，和上面那段代码功能相同，形式不同
        while (!isCentralCoordinatorUpdateComplete) {
            watcherUpdateCompleteCondition.await();
        }


        //（4）获取上一个周期的同步路由表
//        byte[] R_Old_StoreTableByte = zkClient.getData(NMJoinConfigParameters.OLD_R_STORE_ROUTE_TABLE_PATH, false,null);
//        byte[] S_Old_StoreTableByte = zkClient.getData(NMJoinConfigParameters.OLD_S_STORE_ROUTE_TABLE_PATH, false,null);
//        String R_Old_StoreTableString = new String(R_Old_StoreTableByte);
//        String S_Old_StoreTableString = new String(S_Old_StoreTableByte);

        //（5）获取最新存储路由表
        byte[] Both_New_StoreTableByte = zkClient.getData(NMJoinConfigParameters.BOTH_STORE_ROUTE_TABLE_PATH, false,null);
//        byte[] S_New_StoreTableByte = zkClient.getData(NMJoinConfigParameters.S_STORE_ROUTE_TABLE_PATH, false,null);
//        String R_New_StoreTableString = new String(R_New_StoreTableByte);
//        String S_New_StoreTableString = new String(S_New_StoreTableByte);

        //（6）获取最小和最大水位线
        byte[] Min_WaterMarkByte = zkClient.getData(NMJoinConfigParameters.SYNC_MIN_WATERMARK_PATH, false, null);
        byte[] Max_WaterMarkByte = zkClient.getData(NMJoinConfigParameters.SYNC_MAX_WATERMARK_PATH, false, null);
        long Min_SyncWatermark = Long.parseLong(new String(Min_WaterMarkByte));
        long Max_SyncWatermark = Long.parseLong(new String(Max_WaterMarkByte));

        logger.info("[<" + getRuntimeContext().getIndexOfThisSubtask() + ">]当前水位线为："+currentWatermark+"  ；获取的最小水位线为：" + Min_SyncWatermark + "; 最大水位线为：" + Max_SyncWatermark);

        //（7）更新本地连接路由表(用上一个周期结束时的存储路由表关闭上一个周期,其中时间戳用上一个周期所获得的最大水位线加上水位线与元组时间戳的差值来获得)
        R_JoinRouteTable.updateWithLastStoreTable(Both_StoreRouteTable, Max_SyncWatermark + NMJoinConfigParameters.WATERMARK_AFTER_MAX_TIMESTAMP);
        S_JoinRouteTable.updateWithLastStoreTable(Both_StoreRouteTable, Max_SyncWatermark + NMJoinConfigParameters.WATERMARK_AFTER_MAX_TIMESTAMP);

        //（8）更新本地存储路由表（用最新的路由表更新）
        Both_StoreRouteTable.parseFromBytes(Both_New_StoreTableByte);

        //（9）向连接路由表当中插入最新的存储路由表,其中所有项的最小时间戳为当前的最小水位线
        R_JoinRouteTable.insertCurrentStoreTable(Both_StoreRouteTable,Min_SyncWatermark);
        S_JoinRouteTable.insertCurrentStoreTable(Both_StoreRouteTable,Min_SyncWatermark);

        //（10）对连接路由表执行过期操作(输入当前线程的水位线)
        R_JoinRouteTable.removeExpirePartitionEntryWithTimeWindow(currentWatermark);
        S_JoinRouteTable.removeExpirePartitionEntryWithTimeWindow(currentWatermark);

    }

    /**
     *  监控中央协调线程同步信号的监视器，当中央协调线程更改同步标记以通知开始同步时，该方法被调用，将isChangeRouteTable的值置为true
     *      并执行路由表周期性更改同步操作
     */
    class synchronizationWatcher implements Watcher{

        public void process(WatchedEvent event) {
            //将当前元组的处理阻塞（其实用了锁之后可以不用判断标记了，但为了保险起见还是用标记阻塞一下）
            isChangeRouteTable = true;

            //新建线程用于处理存储路由表的更新，这是因为zookeeper的监听机制只有一个线程，
            // 若在zookeeper的监视器线程内处理，则其余的zookeeper监视器会被顺序处理，
            // 因此，若阻塞了其中一个zookeeper回调方法，则其余的所有监视器回调方法均不会被调用
            new Thread(new Runnable() {
                public void run() {
                    watcherLock.lock();
                    try {
                        //进行周期性路由表调整，包括路由表、水位线同步，从zookeeper上拉取最新路由表，以及更改同步标记进行循环监听
                        //在该方法内部会执行 isChangeRouteTable = false; 操作，因此外部便不需要进行该操作了
                        periodChangeRouteTableProcess(currentWatermark);
                        //TODO 测试结果输出
                        myFirstTestSout();

                        //唤醒主线程
                        watcherLockCondition.signalAll();

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        watcherLock.unlock();
                    }
                }
            }).start();

        }
    }

    /**
     * 监控中央线程是否完成最新的路由表的生成与上传，当标记节点发生变化时，表示本节点可以从远程拉取最新的路由表信息了
     *      此时把isCentralCoordinatorUpdateComplete置为True，表示可以进行数据拉取
     */
    class centralCoordinatorUpdateWatcher implements Watcher{

        public void process(WatchedEvent event) {

            //利用锁通知更新存储路由表线程启动
            watcherLock.lock();
            try {
                //首先改变标记
                isCentralCoordinatorUpdateComplete = true;
                //通知更新存储路由表的线程启动
                watcherUpdateCompleteCondition.signalAll();
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                watcherLock.unlock();
            }

        }
    }


    /**
     * 测试用的方法，输出一些信息
     * @throws InterruptedException 默认抛出的异常
     */
    private void myFirstTestSout() throws InterruptedException {

        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        logger.info(indexOfThisSubtask + ">路由表更改完成"+",新存储路由表为-" + Both_StoreRouteTable.soutRouteTable());
        //在指定的路由节点中输出一些信息
        if (indexOfThisSubtask==(NMJoinConfigParameters.NUM_OF_ROUTER - 3)){
            logger.info(""+indexOfThisSubtask + "->的R连接路由表\r\t" + R_JoinRouteTable.soutJoinTable());
//            logger.info(R_JoinRouteTable.soutJoinTable());
            logger.info(""+indexOfThisSubtask + "->的S连接路由表\r\t" + S_JoinRouteTable.soutJoinTable());
//            logger.info(S_JoinRouteTable.soutJoinTable());
        }
        //休眠以方便显示
//        Thread.sleep(10000);
    }


}
