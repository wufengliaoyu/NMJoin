package edu.hit.ftcl.wqh.nomigrationjoin.test;

import edu.hit.ftcl.wqh.nomigrationjoin.NMJoinConfigParameters;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;

public class MyTestSimulateJoiner {

    private static ZooKeeper zkClient;

    private static ArrayList<Long> uploadTimes = new ArrayList<Long>();

    private static int joinerIndex = 1;

    public static void main(String[] args) throws KeeperException, InterruptedException {
        try {
            zkClient = new ZooKeeper(NMJoinConfigParameters.CONNECT_STRING,NMJoinConfigParameters.SESSION_TIMEOUT,null);
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("异常信息 ：MyTestSimulateJoiner 中 zookeeper 初始化连接失败\n");
        }


        for (int i = 0; i < NMJoinConfigParameters.JOINER_TOTAL_NUM; i++){
            uploadTimes.add(0l);
            Stat beginExists = zkClient.exists(NMJoinConfigParameters.SYNC_JOINER_UPLOAD_FLAG, new JoinerTestWatcher(i));
        }



        Thread.sleep(Long.MAX_VALUE);





    }


    private static class JoinerTestWatcher implements Watcher{

        private int index;

        public JoinerTestWatcher(int index) {
            this.index = index;
        }

        public void process(WatchedEvent event) {
            Long aLong = uploadTimes.get(index);
            aLong++;
            uploadTimes.set(index,aLong);
            Stat exists = null;
            try {
                exists = zkClient.exists(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + index, false);
                if (exists == null){
                    zkClient.create(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + index,("JOINER-"+index+uploadTimes.get(index)).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                }else {
                    zkClient.setData(NMJoinConfigParameters.EACH_JOINER_UPLOAD_COMPLETE_FLAG + index,("JOINER-"+index+uploadTimes.get(index)).getBytes(),exists.getVersion());
                }

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("开始处理");



            try {
                zkClient.exists(NMJoinConfigParameters.SYNC_JOINER_UPLOAD_FLAG, new JoinerTestWatcher(index));
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }
}
