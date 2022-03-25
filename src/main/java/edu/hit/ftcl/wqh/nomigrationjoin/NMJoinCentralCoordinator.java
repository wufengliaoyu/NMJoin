package edu.hit.ftcl.wqh.nomigrationjoin;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 中心协调器，负责同步存储路由表以及获取各个joiner的负载并重新构造路由表
 */
public class NMJoinCentralCoordinator extends Thread {
    private static ZooKeeper zkClient;

    //R与S的存储路由表（两个流公用）
    private static StoreRouteTable Both_StoreRouteTable;
//    private static StoreRouteTable S_storeRouteTable;

    //记录远程存储路由表同步的次数
    private static long syncTimes = 0;
    //记录joiner负载上传的次数
    private static long uploadTimes = 0;

    //用于标记所有的Router是否均已经完成同步
    private static ArrayList<Boolean> routerFlagList = new ArrayList<Boolean>();

    //分别记录所有Joiner节点各自负载的字符串列表，主要用于后续更新本地路由表
    //每一个节点存储的负载的格式是 - 键值 负载；-然后列表保存的是所有的节点的该格式信息，即有多行该类型的字符串
    private static ArrayList<String> ALL_JoinerLoadList = new ArrayList<String>();
//    private static ArrayList<String> S_JoinerLoadList = new ArrayList<String>();

    //标记当前的中央处理线程是否需要被杀死的标记，当主流处理进程被终止时，该中央线程不会被系统清理，因此需要手动终止
    private static boolean isCurrentThreadAliveFlag = true;

    //获取logger对象用于日志记录
    private static final Logger logger = Logger.getLogger(NMJoinCentralCoordinator.class);

    //记录上一次进行负载平衡时的键值分布情况，用于判断两次负载平衡操作之间负载的分布变化是否已经达到一定的阈值
    private List<Double> lastKeyWorkloadList = null;

    /**
     * 线程入口，主要负载相关的初始化工作
     */
    @Override
    public void run() {
        //初始化zookeeper
        try {
            zkClient = new ZooKeeper(NMJoinConfigParameters.CONNECT_STRING,NMJoinConfigParameters.SESSION_TIMEOUT,null);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：NMJoinCentralCoordinator 中 zookeeper 初始化连接失败\n");
        }

        //第一次调用NMJoinCentralCoordinator时，若相关前缀节点未被创建，则初始化zookeeper中的相关前缀节点,因为临时节点不能有子节点，所以所有的前缀均为永久节点
        //TODO 由于整个系统的任务调度顺序问题，在第一次启动时，可能有Router或者Joiner在中央线程之前就被启动，此时中央线程的若干前置节点未被创建
        //      可能会产生节点不存在的错误，但在第二次及以后启动时，由于第一次创建的是永久的节点，故而不会出错
        try {

            Stat existsRoot = zkClient.exists(NMJoinConfigParameters.PREFIX_NMJOIN_ROOT_PATH, false);
            Stat existsJoiner = zkClient.exists(NMJoinConfigParameters.PREFIX_NMJOIN_JOINER_PATH, false);
            Stat existsRouter = zkClient.exists(NMJoinConfigParameters.PREFIX_NMJOIN_ROUTER_PATH, false);
            Stat existsMonitor = zkClient.exists(NMJoinConfigParameters.PREFIX_NMJOIN_MONITOR_PATH, false);
            if (existsRoot == null){
                zkClient.create(NMJoinConfigParameters.PREFIX_NMJOIN_ROOT_PATH,"NMJoin".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (existsJoiner == null){
                zkClient.create(NMJoinConfigParameters.PREFIX_NMJOIN_JOINER_PATH,"Joiner".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (existsRouter == null){
                zkClient.create(NMJoinConfigParameters.PREFIX_NMJOIN_ROUTER_PATH,"Router".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            if (existsMonitor == null){
                zkClient.create(NMJoinConfigParameters.PREFIX_NMJOIN_MONITOR_PATH,"Router".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

        } catch (KeeperException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：NMJoinCentralCoordinator 中 zookeeper 前缀节点初始化连接失败\n");
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：NMJoinCentralCoordinator 中 zookeeper 前缀节点初始化连接失败\n");
        }

        //初始化R与S的存储路由表( 路由表的大小为 TOTAL_NUM_OF_TASK )
        Both_StoreRouteTable = new StoreRouteTable(NMJoinConfigParameters.TOTAL_GROUP_NUM_OF_TASK,NMJoinConfigParameters.R_ROUTE_TABLE_KEY_NUM,NMJoinConfigParameters.R_ROUTE_TABLE_MIN_KEY,NMJoinConfigParameters.R_ROUTE_TABLE_MAX_KEY);
//        S_storeRouteTable = new StoreRouteTable(NMJoinConfigParameters.S_NUM_OF_TASK,NMJoinConfigParameters.S_ROUTE_TABLE_KEY_NUM,NMJoinConfigParameters.S_ROUTE_TABLE_MIN_KEY,NMJoinConfigParameters.S_ROUTE_TABLE_MAX_KEY);

        //调用两个存储路由表的初始化方法，对存储路由表进行初始化
        Both_StoreRouteTable.init();
//        S_storeRouteTable.init();

        //将路由表上传到zookeeper中
//        String R_storeRouteTable_String = Both_storeRouteTable.translateToString();
//        String S_storeRouteTable_String = S_storeRouteTable.translateToString();
        byte[] storeTableBytes = Both_StoreRouteTable.translateToBytes();
        try {
            zkClient.create(NMJoinConfigParameters.BOTH_STORE_ROUTE_TABLE_PATH,storeTableBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//            zkClient.create(NMJoinConfigParameters.S_STORE_ROUTE_TABLE_PATH,S_storeRouteTable_String.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("异常信息 ：NMJoinCentralCoordinator 中 zookeeper R/S初始化存储路由表上传失败\n");
        }

        //在zookeeper中创建一些之后将要使用到的节点
        try {
            zkClient.create(NMJoinConfigParameters.OLD_R_STORE_ROUTE_TABLE_PATH,"old_R_storeRouteTable_String".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zkClient.create(NMJoinConfigParameters.OLD_S_STORE_ROUTE_TABLE_PATH,"old_S_storeRouteTable_String".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zkClient.create(NMJoinConfigParameters.SYNC_MIN_WATERMARK_PATH,"R_SYNC_MIN_WATERMARK_PATH".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            zkClient.create(NMJoinConfigParameters.SYNC_MAX_WATERMARK_PATH,"S_SYNC_MIN_WATERMARK_PATH".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("异常信息 ：NMJoinCentralCoordinator 中 zookeeper 初始化一些其他节点的创建时失败\n");
        }

        System.out.println("中央协调线程启动成功！");

        //休眠一段时间后开始进行周期性的路由表调整
        try {
            Thread.sleep(NMJoinConfigParameters.COORDINATE_PERIOD);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        isCurrentThreadAliveFlag = true;
        //进行周期性的路由表调整（只有在当前的线程被判断仍然需要执行时才会进行调整，若主流处理程序终止，则该中央线程退出）
        //NMJoinConfigParameters.COORDINATE_PERIOD : 休眠时间，也就是两次调整存储路由表之间的时间间隔，可以随着数据输入的波动而改变调整路由表的时间间隔
        while (isCurrentThreadAliveFlag){
            //调用周期性调整函数
            try {
                periodAdjustment();
            } catch (KeeperException e) {
                e.printStackTrace();
                System.err.println("异常信息 ：NMJoinCentralCoordinator 中 periodAdjustment 失败\n");
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.err.println("异常信息 ：NMJoinCentralCoordinator 中 periodAdjustment 失败\n");
            }
            //休眠一段时间
            try {
                Thread.sleep(NMJoinConfigParameters.COORDINATE_PERIOD);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        //主程序退出时关闭zookeeper
        try {
            zkClient.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //TODO 输出测试信息
        logger.info("中线线程已经退出");

    }

    /**
     * 周期性的根据所有joiner的负载信息调整存储路由表的结构
     * 具体步骤如下：
     *      通知所有的joiner上传负载信息到zookeeper上
     *      从zookeeper上收集所有joiner的负载信息
     *      调整本地路由表
     *      发送信号给所有的Router，让所有Router在zookeeper上同步存储路由表
     *      获取zookeeper上的存储路由表
     *      构建新的存储路由表
     *      将新的路由表上传到zookeeper上
     *      休眠
     */
    private void periodAdjustment() throws KeeperException, InterruptedException {

        //（-0）需要判断当前Router与Joiner是否存活，当Flink终止流处理进程时，该中央协调线程并不会被终止，因此在第二次启动时会发生错误
        //在此通过监控zookeeper上对应的Joiner0的临时节点是否存在来判断是否要退出中央协调线程
        //TODO 这里采用监控的Joiner0，在集群故障时可能出错，可能有更好的方法，
        //      此外过早的监视该节点，如果该中央协调进程启动过早，可能在Joiner启动前启动，那么会发生错误，直接退出，此处是因为现在中央协调会延迟启动，因此暂时不会出错
        Stat isMainLived = zkClient.exists(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + 0, false);
        //如果joiner0被判断死亡，则该中央线程也通过标记isCurrentThreadAliveFlag终止
        if (isMainLived == null) {
            isCurrentThreadAliveFlag = false;
            return;
        }

        //（1）收集所有Joiner负载信息

        try {
            collectJoinerLoad();
        } catch (KeeperException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：NMJoinCentralCoordinator 中 收集 Joiner负载信息 失败\n");
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：NMJoinCentralCoordinator 中  收集 Joiner负载信息 失败\n");
        }

        //（2）根据负载信息判断当前是否要更新路由表，如果要更新路由表，则计算新的路由表

        //根据各个节点的负载列表，判断是否要进行路由表更改，并且在其中更改本地的存储路由表
        boolean isUpdate = updateLocalStoreTable();

        //TODO 根据设置判断是否要开启负载平衡
        if (!NMJoinConfigParameters.IS_WORKLOAD_BALANCE_TURN_ON){
            isUpdate = false;
        }

        if(isUpdate){ //如果需要更改路由表，同步zookeeper
            //（3）同步zookeeper上的存储路由表
            //TODO 用于让改变路由表的次数更加的明显一点
            System.out.println("进行路由表更改，负载平衡处理");
            noticeAndUpdateSyncRouterStoreTable();
        }else{ //如果不需要更改路由表，直接返回
            return;
        }



    }

    /**
     * 收集所有joiner的负载信息
     */
    private void collectJoinerLoad() throws KeeperException, InterruptedException {

        //监听各个Joiner标记节点的数据，初始化R与S的负载存储列表各项均为null，若收到数据则置为相应的数据
        ALL_JoinerLoadList.clear();
//        S_JoinerLoadList.clear();
        for (int i = 0; i < NMJoinConfigParameters.JOINER_TOTAL_NUM; i++){
            /*
            if (i<NMJoinConfigParameters.R_NUM_OF_TASK){//若为存储R的joiner节点（即前几个节点），初始化R负载表
                ALL_JoinerLoadList.add(null);
            }else {//若为存储S的joiner节点（即后几个节点），初始化S负载表
                S_JoinerLoadList.add(null);
            }
             */
            ALL_JoinerLoadList.add(null);
            //监控对应的joiner节点标记
            zkClient.exists(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + i , new JoinerLoadFlagListWatch(i));
        }


        //通知各个joiner节点进行数据上传
        Stat beginExists = zkClient.exists(NMJoinConfigParameters.SYNC_JOINER_UPLOAD_FLAG, false);
        if (beginExists == null){
            zkClient.create(NMJoinConfigParameters.SYNC_JOINER_UPLOAD_FLAG,("UPLOAD-" + uploadTimes).getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }else {
            zkClient.setData(NMJoinConfigParameters.SYNC_JOINER_UPLOAD_FLAG,("UPLOAD-" + uploadTimes).getBytes(),beginExists.getVersion());
        }

        //记录上传的次数
        uploadTimes++;


        for (int i = 0; i < NMJoinConfigParameters.JOINER_TOTAL_NUM; i++){
            /*
            if (i < NMJoinConfigParameters.R_NUM_OF_TASK){//若为存储R的joiner节点（即前几个节点）,观察对应的负载列表项是否都已被改变
                //若对应位置的项依旧为null，则代表程序仍然未获取到该项的数据，则继续等待
                while (ALL_JoinerLoadList.get(i) == null){
                    Thread.sleep(10);
                }
            }else{//若为存储S的joiner节点（即后几个节点），同上
                //若对应位置的项依旧为null，则代表程序仍然未获取到该项的数据，则继续等待
                while (S_JoinerLoadList.get(i - NMJoinConfigParameters.R_NUM_OF_TASK) == null){
                    Thread.sleep(10);
                }
            }
             */

            //若对应位置的项依旧为null，则代表程序仍然未获取到该项的数据，则继续等待
            while (ALL_JoinerLoadList.get(i) == null){
                Thread.sleep(10);
            }

        }

        //至此，已经获取了所有joiner节点的负载

        //TODO 测试信息输出
        StringBuilder loadStringBuilder = new StringBuilder();
        for (String s : ALL_JoinerLoadList){
            loadStringBuilder.append(s);
            loadStringBuilder.append("--");
        }
        logger.info("测试，从各个Joiner获取的原始负载信息为：--" + loadStringBuilder.toString());

    }

    /**
     * 根据获取的所有joiner的负载信息判断是否需要更改本地的路由表
     * 若判断需要改变本地的路由表，则对本地路由表进行更新，同时返回True
     * 若判断不需要改变，则返回False
     * TODO 目前是一个试运行的方案，之后可以进行优化
     * @return True:已经对本地路由表进行了更改，需要将本地路由表同步到zookeeper上
     *          False：本个调整周期内不需要做更改，也就不需要将本地路由表同步到zookeeper上
     */
    private boolean updateLocalStoreTable(){
        //根据从zookeeper上获取的各个joiner上传的各键值分布的负载字符串信息，以及阈值，分别对R与S的存储路由表进行负载不平衡判断，并在判定为负载倾斜时对存储路由表进行调整
        boolean isChanged = adjustTheStoreRouteTableForWorkloadBalance(Both_StoreRouteTable, ALL_JoinerLoadList, NMJoinConfigParameters.WORKLOAD_BALANCE_THRESHOLD);
//        boolean is_S_Changed = adjustTheStoreRouteTableForWorkloadBalance(S_storeRouteTable, S_JoinerLoadList, NMJoinConfigParameters.WORKLOAD_BALANCE_THRESHOLD);
        //只要R或S其中的一个存储路由表被更改了，就要进行所有Router的存储路由表同步
        return isChanged;
    }

    /**
     * 通知所有的Router将其各自的存储路由表上传到zookeeper中，并且从zookeeper上获取同步后的存储路由表,
     * 之后根据本地的重构后的路由表构建最终的路由表并上传到zookeeper上
     */
    private void noticeAndUpdateSyncRouterStoreTable() throws KeeperException, InterruptedException {

        //（1）将所有的routerFlagList标记位均置为false，同时对所有的zookeeper上对应的Router同步标记进行监听，若标记被改变，则修改对应标记位为true
        routerFlagList.clear();
        for (int i = 0;i < NMJoinConfigParameters.NUM_OF_ROUTER;i++){
            routerFlagList.add(false);
            zkClient.exists(NMJoinConfigParameters.EACH_ROUTER_SYNC_COMPLETE_FLAG + i,new RouterFlagListWatch(i));
        }

        //（2）将zookeeper上的保存最小水位线的节点的值置为Long的最大值，用于更新最小水位线
        Stat R_Watermark_exists = zkClient.exists(NMJoinConfigParameters.SYNC_MIN_WATERMARK_PATH, false);
        zkClient.setData(NMJoinConfigParameters.SYNC_MIN_WATERMARK_PATH,(""+Long.MAX_VALUE).getBytes(),R_Watermark_exists.getVersion());
        //-->（2）将zookeeper上的保存最大水位线的节点的值置为Long的最小值，用于更新最大水位线
        Stat S_Watermark_exists = zkClient.exists(NMJoinConfigParameters.SYNC_MAX_WATERMARK_PATH, false);
        zkClient.setData(NMJoinConfigParameters.SYNC_MAX_WATERMARK_PATH,(""+Long.MIN_VALUE).getBytes(),S_Watermark_exists.getVersion());

        //（3）设置zookeeper上同步开启的标记，以通知所有的Router开始进行路由表同步
        Stat exists = zkClient.exists(NMJoinConfigParameters.SYNC_ROUTER_STORE_TABLE_FLAG, false);
        if (exists == null){
            zkClient.create(NMJoinConfigParameters.SYNC_ROUTER_STORE_TABLE_FLAG,"SYNC".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }else {
            zkClient.setData(NMJoinConfigParameters.SYNC_ROUTER_STORE_TABLE_FLAG,("SYNC-"+syncTimes).getBytes(),exists.getVersion());
        }


        //-->（9）上传最终的同步路由表,(由于本地不需要获取zookeeper上的任何信息了，因此可以直接将存储路由表上传)
        //     TODO 这步可以放在（3）之前执行，应该可以节省一部分时间，若是后期发现系统整体同步时间过长，可以修改这里
        byte[] bytes = Both_StoreRouteTable.translateToBytes();
        Stat R_exists = zkClient.exists(NMJoinConfigParameters.BOTH_STORE_ROUTE_TABLE_PATH, false);
        zkClient.setData(NMJoinConfigParameters.BOTH_STORE_ROUTE_TABLE_PATH,bytes,R_exists.getVersion());


        //（4）等待所有标记位均置为true，即所有Router同步完成
        for (int i = 0;i<NMJoinConfigParameters.NUM_OF_ROUTER;i++){
            while (!routerFlagList.get(i)){
                Thread.sleep(10);
            }
        }

        /*
        //（5）获取zookeeper上的同步路由表
        byte[] R_StoreTableByte = zkClient.getData(NMJoinConfigParameters.BOTH_STORE_ROUTE_TABLE_PATH, false,null);
        byte[] S_StoreTableByte = zkClient.getData(NMJoinConfigParameters.S_STORE_ROUTE_TABLE_PATH, false,null);
        String R_StoreTableString = new String(R_StoreTableByte);
        String S_StoreTableString = new String(S_StoreTableByte);

        //（6）将同步路由表上传到zookeeper中用于保存上一个周期结束时的同步存储路由表的节点
        Stat Old_R_exists = zkClient.exists(NMJoinConfigParameters.OLD_R_STORE_ROUTE_TABLE_PATH, false);
        zkClient.setData(NMJoinConfigParameters.OLD_R_STORE_ROUTE_TABLE_PATH,R_StoreTableString.getBytes(),Old_R_exists.getVersion());
        Stat Old_S_exists = zkClient.exists(NMJoinConfigParameters.OLD_S_STORE_ROUTE_TABLE_PATH, false);
        zkClient.setData(NMJoinConfigParameters.OLD_S_STORE_ROUTE_TABLE_PATH,S_StoreTableString.getBytes(),Old_S_exists.getVersion());
         */

        /*
        //（7）获取zookeeper上的同步水位线
        byte[] Min_WaterMarkByte = zkClient.getData(NMJoinConfigParameters.SYNC_MIN_WATERMARK_PATH, false, null);
        byte[] Max_WaterMarkByte = zkClient.getData(NMJoinConfigParameters.SYNC_MAX_WATERMARK_PATH, false, null);
        long Min_SyncWatermark = Long.parseLong(new String(Min_WaterMarkByte));
        long Max_SyncWatermark = Long.parseLong(new String(Max_WaterMarkByte));
         */

        /*
        //（8）构建最终的同步路由表字符串，同时更新本地路由表为最新
        String R_ResultRouteTableString = Both_StoreRouteTable.centralCoordinatorUpdateRemoteTableWithString(R_StoreTableString,Min_SyncWatermark);
        String S_ResultRouteTableString = S_storeRouteTable.centralCoordinatorUpdateRemoteTableWithString(S_StoreTableString,Max_SyncWatermark);
         */




//        Stat S_exists = zkClient.exists(NMJoinConfigParameters.S_STORE_ROUTE_TABLE_PATH, false);
//        zkClient.setData(NMJoinConfigParameters.S_STORE_ROUTE_TABLE_PATH,S_ResultRouteTableString.getBytes(),S_exists.getVersion());



        //（10）改变同步标记，以通知所有的Router同步存储路由表过程结束，各个Router可以从zookeeper上拉取最新的存储路由表了
        int syncVersion = zkClient.exists(NMJoinConfigParameters.SYNC_ROUTER_STORE_TABLE_FLAG, false).getVersion();
        zkClient.setData(NMJoinConfigParameters.SYNC_ROUTER_STORE_TABLE_FLAG,("SYNC-"+syncTimes+"-ok").getBytes(),syncVersion);
        syncTimes++;

        //TODO test
        logger.info("中央线程调整后的路由表");
        logger.info(Both_StoreRouteTable.soutRouteTable());

    }

    /**
     * 根据给出的各个节点的负载字符串信息，对指定的存储路由表进行调整，以达到负载平衡的结果
     * @param storeRouteTable 要调整的存储路由表，分别可以指定为 R 与 S 的存储路由表
     * @param workloadStringList 表明对应存储路由表当前的负载分布状况的字符串，是通过zookeeper获取的由各个joiner上传的信息，其中保存着多个joiner的负载字符串
     * @param threshold 负载不平衡的阈值，当负载不平衡程度超过该阈值时，需要进行负载平衡的调整
     * @return 表示指定的路由表是否需要（并且已经）进行调整；true：需要调整 ；false：不需要
     */
    private boolean adjustTheStoreRouteTableForWorkloadBalance(StoreRouteTable storeRouteTable, ArrayList<String> workloadStringList,double threshold) {
        //（1）获取包含每个键值对应负载信息的列表
        List<Double> workloadListOfEachKey = getWorkloadOfEachKey(storeRouteTable, workloadStringList);

        //TODO Test输出
        logger.info("中央线程的键值负载列表" + workloadListOfEachKey);

        //( -1.1 ) 计算负载分布之间的变化是否达到阈值，若未达到，则直接返回false
        if (!isDistributeChangeSatisfyThreshold(workloadListOfEachKey)) {
            logger.info("当前负载分布变化未超过规定的阈值，不进行新的负载平衡计算");
            return false;
        }
        logger.info("当前负载分布变化超过规定的阈值，将进行新的负载平衡计算");

        //（2）获取分区负载列表，其中包括分区负载，分区键值范围，分区调整方案（好像没有分区调整方案）
        List<AdjustRouteTablePartitionEntry> partitionWorkloadList = getPartitionWorkloadList(storeRouteTable, workloadListOfEachKey);

        //TODO Test
        logger.info("中央线程的分区负载列表");
        for (AdjustRouteTablePartitionEntry e : partitionWorkloadList) {
            logger.info(e);
        }

        //（3）判断当前系统是否存在负载不平衡，若存在，则进行负载平衡，否则，直接返回false表示不需要进行负载平衡
        if (isWorkloadUnbalance(partitionWorkloadList, threshold)) {

            //（4）若负载不平衡，则需要首先计算负载的调度方案方案，并根据给出的负载调度方案，对指定的存储路由表进行调整
            computeSchedulingSchemeAndAdjustStoreRouteTable(storeRouteTable,partitionWorkloadList, workloadListOfEachKey);
            logger.info("当前存在负载不平衡，中央线程已完成负载均衡的新存储路由表生成");

            //返回true，表明当前需要并且已经进行了存储路由表的更改
            return true;
        } else {
            logger.info("当前不存在负载不平衡，未进行新存储路由表生成");
            //返回false，表明当前不需要进行负载平衡，即没有对存储路由表进行过更改
            return false;
        }

    }


    /**
     * 判断两次该表路由表周期之间，数据的分布改变是否超过了规定的阈值，若超过阈值，才会进行新的路由表更新
     * @param workloadListOfEachKey 新传入的键值负载列表
     * @return 返回true，则代表键值分布变化很大，需要进行进一步的负载平衡操作
     */
    private boolean isDistributeChangeSatisfyThreshold(List<Double> workloadListOfEachKey) {

        //如果是第一次进行调整，则直接返回true，并更新最新调整的键值负载列表
        if (lastKeyWorkloadList == null) {
            lastKeyWorkloadList = new ArrayList<>(workloadListOfEachKey);
            logger.info("lastKeyWorkloadList为空，初始化");
            return true;
        }

        //遍历最新的和上一次的键值负载列表，计算所有键值之间的差的和
        //分别记录上次的总和以及二者之间的差，用于计算比率
        Double totalLoad = 0.0;
        Double totalKeyLoadDifferent = 0.0;
        for (int i = 0; i < workloadListOfEachKey.size(); i++) {
            Double newKeyLoad = workloadListOfEachKey.get(i);
            Double lastKeyLoad = lastKeyWorkloadList.get(i);
            totalLoad += lastKeyLoad;
            totalKeyLoadDifferent += Math.abs(lastKeyLoad - newKeyLoad);
        }

        if (totalLoad == 0) {
            lastKeyWorkloadList.clear();
            lastKeyWorkloadList.addAll(workloadListOfEachKey);
            logger.info("totalLoad == 0，分布变化返回true");
            return true;
        }

        //如果平均键值负载不平衡程度大于阈值，则更新上一次的键值负载表，返回true
        double diff = totalKeyLoadDifferent / totalLoad;
        if (diff > NMJoinConfigParameters.DISTRIBUTE_CHANGE_THRESHOLD) {
            lastKeyWorkloadList.clear();
            lastKeyWorkloadList.addAll(workloadListOfEachKey);
            logger.info("本次负载平衡计算与上次进行负载平衡时相比，负载分布变化程度超过阈值，为：" + diff);
            return true;
        } else {
            logger.info("本次负载平衡计算与上次进行负载平衡时相比，负载分布变化程度-未超过-阈值，为：" + diff);
            return false;
        }

    }


    /**
     * 根据通过zookeeper获得的保存有各个joiner节点若干键值负载信息的字符串，构建包含每一个键值对应负载信息的列表并返回，用于后续进行负载平衡
     * @param storeRouteTable 需要统计每个键值负载信息的存储列表（分别为 R 与 S）
     * @param workloadStringList 通过zookeeper获得的保存有各个joiner节点若干键值负载信息的字符串列表（若干行，每行对应一个joiner节点）
     * @return 包含每一个键值对应负载信息的列表
     */
    private List<Double> getWorkloadOfEachKey(StoreRouteTable storeRouteTable, ArrayList<String> workloadStringList) {
        //保存每一个键值负载的数组
        ArrayList<Double> loadOfEachKey = new ArrayList<Double>(storeRouteTable.getNumOfKey());
        //将每一个键值的负载初始化为0
        for (int i = 0; i < storeRouteTable.getNumOfKey(); i++) {
            loadOfEachKey.add(0.0);
        }

        //获取每一个joiner的负载字符串 格式为（键值 负载；键值 负载；...）
        for (String keyLoadString : workloadStringList) {
            //如果对应的字符串为空，则表明对应的分区没有接收到负载，跳过该字符串
            if (keyLoadString.length() == 0) {
                continue;
            }
            //获取每一个键值负载对，格式为（键值 负载）
            String[] splitKeyLoadStringSet = keyLoadString.split(";");
            for (String splitKeyLoadString : splitKeyLoadStringSet) {
                //分别获取键值与负载
                String[] s = splitKeyLoadString.split(" ");
                //获取键值
                int keyIndex = Integer.parseInt(s[0]);
                //获取负载
                double workloadOfTheKey = Double.parseDouble(s[1]);
                //在loadOfEachKey中查找对应键值的负载，在其值的基础上加上新获得的负载
                double newWorkload = loadOfEachKey.get(keyIndex) + workloadOfTheKey;
                loadOfEachKey.set(keyIndex, newWorkload);

            }
        }

        //返回保存有各个键值负载的列表
        return loadOfEachKey;
    }

    /**
     * 获取分区负载列表，记录每一个分区的相关信息，包括负载，键值范围 以及 后续的调整负载方案，在进行负载调度时该方法被调用
     * @param storeRouteTable 要进行调度的存储路由表
     * @param workloadListOfEachKey 包含每一个键值对应负载信息的列表
     * @return 构建的分区负载列表，记录每一个分区的相关信息，包括负载，键值范围 以及 后续的调整负载方案
     */
    private List<AdjustRouteTablePartitionEntry> getPartitionWorkloadList(StoreRouteTable storeRouteTable, List<Double> workloadListOfEachKey) {
        //获取每个joiner分区所包含的键值范围
        List<Tuple3<Integer, Integer, Integer>> keyRangeOfEachPartition = storeRouteTable.getKeyRangeOfEachPartition();

        //构建分区负载列表，记录每一个分区的相关信息，包括负载，键值范围 以及 后续的调整负载方案
        List<AdjustRouteTablePartitionEntry> partitionWorkloadList = new ArrayList<AdjustRouteTablePartitionEntry>();

        //循环构建分区负载列表
        for (int i = 0; i < keyRangeOfEachPartition.size(); i++) {
            Tuple3<Integer, Integer, Integer> partitionRange = keyRangeOfEachPartition.get(i);
            //如果当前分区未被分配键值，则将范围两端置为null，并将负载置为0
            if (partitionRange.f1 == null || partitionRange.f2 == null) {
                partitionWorkloadList.add(new AdjustRouteTablePartitionEntry(i, 0, null, null));
                continue;
            }

            //初始化左边界
            int index = partitionRange.f1;
            //初始化负载
            double workload = 0.0;
            //当左边界不等于右边界，则将左边界依次加一，直到右边界，并累加期间的负载
            while (index != partitionRange.f2) {
                workload += workloadListOfEachKey.get(index);
                //由于会循环，因此要进行取余
                index = (index + 1) % storeRouteTable.getNumOfKey();
            }
            //当跳出循环时，此时指针指向右边界，而右边界的值未被计算，因此需要单独计算
            workload += workloadListOfEachKey.get(index);

            //构建新的分区列表项并插入到 分区负载列表 partitionWorkloadList 中
            partitionWorkloadList.add(new AdjustRouteTablePartitionEntry(i, workload, partitionRange.f1, partitionRange.f2));
        }

        //返回包含每个分区信息的列表，其中包括分区负载，分区键值范围，分区调整方案
        return partitionWorkloadList;
    }

    /**
     * 根据给出的分区负载列表，以及用户自定义的负载不平衡上限，判断当前系统是否存在负载不平衡的现象
     * 如果 --（最大负载 - 平均负载）/平均负载 -- 的值大于给定 -- 阈值 --，则判定存在负载倾斜，返回true
     * @param partitionWorkloadList 当前存储路由表的分区负载列表
     * @param threshold 用户自定义的负载不平衡上限
     * @return 当系统存在负载不平衡时，返回 true；若系统负载平衡，即不需要进行负载调整，则返回false
     */
    private boolean isWorkloadUnbalance(List<AdjustRouteTablePartitionEntry> partitionWorkloadList, double threshold) {

        //记录具有最大负载的分区号
        int maxLoadPartitionIndex = 0;
        //记录最大的负载(初始为分区0的负载)
        double maxWorkload = partitionWorkloadList.get(maxLoadPartitionIndex).getWorkload();

        //统计所有分区的总负载
        double totalWorkload = 0.0;

        //遍历所有分区的负载，计算总负载以及找出最大的负载
        for (int i = 0; i < partitionWorkloadList.size(); i++) {
            //获取当前分区的负载
            double currentWorkload = partitionWorkloadList.get(i).getWorkload();
            //如果当前分区的负载大于最大值，则更新负载最大值以及负载最大值所在的索引
            if (currentWorkload > maxWorkload) {
                maxWorkload = currentWorkload;
                maxLoadPartitionIndex = i;
            }

            //更新总负载
            totalWorkload += currentWorkload;
        }

        //计算平均负载
        double averageWorkload = totalWorkload/partitionWorkloadList.size();
        if (averageWorkload == 0) {
            logger.info("平均负载为0，不需要进行负载迁移");
            return false;
        }

        //如果 --（最大负载 - 平均负载）/平均负载 -- 的值大于给定 -- 阈值 --，则判定存在负载倾斜，返回true
        double currentUnBalanceFactor = Math.abs(maxWorkload - averageWorkload) / averageWorkload;
        logger.info("当前负载不均衡系数为：" + currentUnBalanceFactor + ",阈值为：" + threshold);
        if (currentUnBalanceFactor > threshold) {
            return true;
        } else {
            return false;
        }

    }

    /**
     * 根据给出的 分区负载信息列表 以及 键值负载信息列表 计算分区调度方案并根据该方案调整 存储路由表
     * @param storeRouteTable 要被调整的路由表
     * @param partitionWorkloadList 分区负载列表，其中包括分区负载，分区键值范围，分区调整方案
     * @param workloadListOfEachKey 包含每个键值对应负载信息的列表
     */
    private void computeSchedulingSchemeAndAdjustStoreRouteTable(StoreRouteTable storeRouteTable,List<AdjustRouteTablePartitionEntry> partitionWorkloadList, List<Double> workloadListOfEachKey) {
        //统计总负载
        double totalWorkload = 0.0;
        for (AdjustRouteTablePartitionEntry p : partitionWorkloadList) {
            totalWorkload += p.getWorkload();
        }

        //计算平均负载
        double averageWorkload = totalWorkload/partitionWorkloadList.size();

        //构建新的存储路由表
        ArrayList<Integer> newStoreRouteTable = new ArrayList<Integer>(storeRouteTable.getNumOfKey());
        //初始化，将所有项置为0
        for (int i = 0; i < storeRouteTable.getNumOfKey(); i++) {
            newStoreRouteTable.add(0);
        }

        //当前正在进行负载分配的分区号（从0开始）
        int currentPartition = 0;
        //当前分区已经被分配的负载总量
        double currentPartitionWorkload = 0.0;

        //跳过前面一些未被分配键值的分区
        while (partitionWorkloadList.get(currentPartition).getLeftKey() == null) {
            currentPartition++;
        }

        //获取currentPartition的左端键值，作为所有键值重新分配的最左边的键值，从这个键值开始进行键值重新分配
        int firstKeyIndex = partitionWorkloadList.get(currentPartition).getLeftKey();

        //当前正在进行负载分配的键值索引号
        int currentKeyIndex = firstKeyIndex;
        //构建最后一个要分配的键值索引，当遍历到该键值时，说明所有键值都已经被分配了
        int finalKeyIndex;
        if (currentKeyIndex == 0) {
            finalKeyIndex = storeRouteTable.getNumOfKey() - 1;
        } else {
            finalKeyIndex = currentKeyIndex-1;
        }



        //循环分配键值的分区
        while (currentKeyIndex != finalKeyIndex) {
            //将当前键值分配给当前分区
            newStoreRouteTable.set(currentKeyIndex, currentPartition);
            //如果算上当前键值的负载后，当前分区的总负载超过（或等于）了平均负载，则下一个键值的分配需要从下一个分区开始，并且总负载清零
            if (currentPartitionWorkload + workloadListOfEachKey.get(currentKeyIndex) >= averageWorkload) {
                //循环分配下一个分区
                currentPartition = (currentPartition + 1) % partitionWorkloadList.size();
                //重置总负载
                currentPartitionWorkload = 0.0;
            } else { //如果算上当前键值的负载后，当前分区的总负载依然小于平均负载，则下一个键值任然分配给当前分区，并且更新当前分区的负载
                currentPartitionWorkload += workloadListOfEachKey.get(currentKeyIndex);
            }

            //循环遍历所有键值
            currentKeyIndex = (currentKeyIndex + 1) % storeRouteTable.getNumOfKey();
        }


        //当跳出上面的循环后，finalKeyIndex仍然没有被分配分区，因此在这里要单独为其分配分区
        newStoreRouteTable.set(finalKeyIndex, currentPartition);

        //至此，已经得到了新的分区方案newStoreRouteTable

        /*
        //构建存储路由表字符串，用于更新本地存储路由表
        StringBuilder storeRouteTableString = new StringBuilder();
        for (Integer partition : newStoreRouteTable) {
            storeRouteTableString.append("" + partition + " " + "0" + ";");
        }
         */

        //将新构建的存储路由表序列化为字节数组，用于构建本地的存储路由表
        byte[] serializeStoreTable = new MySerializerForZookeeperTranslate<ArrayList<Integer>>().serialize(newStoreRouteTable);

        //根据存储路由表字符串更新本地存储路由表
       storeRouteTable.parseFromBytes(serializeStoreTable);

    }


    @Test
    public void workloadBalanceTest1() {
        StoreRouteTable storeRouteTable = new StoreRouteTable(8,20,10,1000);
        storeRouteTable.init();
//        storeRouteTable.forwardSpecifyJoinerSpecifyNumKey(2,3,1);
//        storeRouteTable.backwardSpecifyJoinerSpecifyNumKey(2,2,1);
        storeRouteTable.soutRouteTable();
        ArrayList<String> workloadStringList = new ArrayList<String>();

        for (int i = 0; i < storeRouteTable.getNumOfStoreJoiner(); i++) {
            StringBuilder stringBuilder = new StringBuilder();
            for (int j = 0; j <= storeRouteTable.getNumOfKey() / storeRouteTable.getNumOfStoreJoiner() + 1; j++) {
                int i1 = i * (storeRouteTable.getNumOfKey() / storeRouteTable.getNumOfStoreJoiner());
                if (i1 > storeRouteTable.getNumOfKey() - 1) {
                    i1 = storeRouteTable.getNumOfKey() - 1;
                }
                stringBuilder.append("" + (i1+j) + " " + "1;");
            }
            workloadStringList.add(stringBuilder.toString());
        }


        List<Double> workloadOfEachKey = getWorkloadOfEachKey(storeRouteTable, workloadStringList);

        System.out.println(workloadOfEachKey);
        List<Tuple3<Integer, Integer, Integer>> keyRangeOfEachPartition = storeRouteTable.getKeyRangeOfEachPartition();
        System.out.println(keyRangeOfEachPartition);
        List<AdjustRouteTablePartitionEntry> partitionWorkloadList = getPartitionWorkloadList(storeRouteTable, workloadOfEachKey);
        for (AdjustRouteTablePartitionEntry e :partitionWorkloadList
        ) {
            System.out.println(e);
        }

        boolean workloadUnbalance = isWorkloadUnbalance(partitionWorkloadList, 0.5);
        System.out.println(workloadUnbalance);

        computeSchedulingSchemeAndAdjustStoreRouteTable(storeRouteTable, partitionWorkloadList, workloadOfEachKey);
        storeRouteTable.soutRouteTable();

        List<AdjustRouteTablePartitionEntry> partitionWorkloadList2 = getPartitionWorkloadList(storeRouteTable, workloadOfEachKey);
        for (AdjustRouteTablePartitionEntry e :partitionWorkloadList2
        ) {
            System.out.println(e);
        }


    }

    /**
     * 用于在负载平衡调整存储路由表的阶段保存每一个分区的相关信息
     */
    static class AdjustRouteTablePartitionEntry{
        //当前分区号以及负载
        private int partition;
        private double workload;
        //当前分区分配的键值范围
        private Integer leftKey;
        private Integer rightKey;
        //当前分区将要失去的左侧键值数量
        private int forwardNum;
        //当前分区将要失去的右侧键值数量
        private int backwardNum;

        public AdjustRouteTablePartitionEntry() {
        }

        public AdjustRouteTablePartitionEntry(int partition, double workload, Integer leftKey, Integer rightKey) {
            this.partition = partition;
            this.workload = workload;
            this.leftKey = leftKey;
            this.rightKey = rightKey;
            this.forwardNum = 0;
            this.backwardNum = 0;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public double getWorkload() {
            return workload;
        }

        public void setWorkload(double workload) {
            this.workload = workload;
        }

        public Integer getLeftKey() {
            return leftKey;
        }

        public void setLeftKey(Integer leftKey) {
            this.leftKey = leftKey;
        }

        public Integer getRightKey() {
            return rightKey;
        }

        public void setRightKey(Integer rightKey) {
            this.rightKey = rightKey;
        }

        public int getForwardNum() {
            return forwardNum;
        }

        public void setForwardNum(int forwardNum) {
            this.forwardNum = forwardNum;
        }

        public int getBackwardNum() {
            return backwardNum;
        }

        public void setBackwardNum(int backwardNum) {
            this.backwardNum = backwardNum;
        }

        @Override
        public String toString() {
            return "AdjustRouteTablePartitionEntry{" +
                    "partition=" + partition +
                    ", workload=" + workload +
                    ", leftKey=" + leftKey +
                    ", rightKey=" + rightKey +
                    ", forwardNum=" + forwardNum +
                    ", backwardNum=" + backwardNum +
                    '}';
        }
    }


    /**
     * 监视指定的Router在zookeeper上对应的标记是否被修改过，以判断该Router是否完成了同步
     */
    static class RouterFlagListWatch implements Watcher {
        private int routerIndex;

        public RouterFlagListWatch(int routerIndex) {
            this.routerIndex = routerIndex;
        }

        public void process(WatchedEvent event) {
            routerFlagList.set(routerIndex,true);
        }
    }

    /**
     * 监视指定的 joiner 在zookeeper上对应的标记是否被修改过数据，以判断该 Joiner 的负载是否完成了上传，并将上传的负载存储到对应类型的本地负载列表中
     */
    static class JoinerLoadFlagListWatch implements Watcher{
         //标记是哪个joiner节点
        private int joinerIndex;

        public JoinerLoadFlagListWatch(int joinerIndex) {
            this.joinerIndex = joinerIndex;
        }

        public void process(WatchedEvent event) {
            //获取该监听类对应的joiner节点的数据
            byte[] data = new byte[0];
            try {
                data = zkClient.getData(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + joinerIndex, false, null);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String load = new String(data);

            /*
            //将获取的负载数据存储到对应类型的负载列表中
            if (joinerIndex<NMJoinConfigParameters.R_NUM_OF_TASK){//如果是存储R元组的列表
                ALL_JoinerLoadList.set(joinerIndex,load);
            } else {//如果是存储S元组的列表
                //需要减去前面R节点的数量，剩下的才是S节点的正确编号
                S_JoinerLoadList.set(joinerIndex - NMJoinConfigParameters.R_NUM_OF_TASK,load);
            }
             */

            ALL_JoinerLoadList.set(joinerIndex,load);

        }
    }

}
