package edu.hit.ftcl.wqh.BPlusTreeTest;

import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class test {
//    public static void main(String[] args) {
//        MySubBPlusTree<String, Integer> integerIntegerMySubBPlusTree = new MySubBPlusTree<String, Integer>();
////        integerIntegerMySubBPlusTree.insert(15,15,10l);
////        integerIntegerMySubBPlusTree.insert(14,14,10l);
////        integerIntegerMySubBPlusTree.insert(13,13,10l);
//        Random random = new Random();
//        long time1 = System.nanoTime();
//        for(int i = 0;i<1000;i++){
//            int t = random.nextInt(101);
//            integerIntegerMySubBPlusTree.insert("tuple:"+t,t,10l);
//        }
//        long time2 = System.nanoTime();
//        MySubBPlusTree<String, Integer>.LinkListNode first = integerIntegerMySubBPlusTree.getFirst();
//        int num = 1;
//        first=first.next;
//        while (first.next!=null){
//            System.out.println(first.item + " " + first.key);
//            System.out.println("     " + num);
//            first=first.next;
//            num++;
//        }
//
//        System.out.println("长度： "+integerIntegerMySubBPlusTree.getLenght()+" 高度： "+integerIntegerMySubBPlusTree.hight);
//        System.out.println("-----------------------------------------------------------------------------------------");
//
//        List<String> range = integerIntegerMySubBPlusTree.findRange(100, 100, 0l);
//        if(range==null){
//            System.out.println("结果为空");
//        }else{
//            for(String e : range){
//                System.out.println(e);
//            }
//        }
//
//        System.out.println("插入耗时: " + (time2 - time1));
//
//    }


//    public static void main(String[] args) {
//        MySubBPlusTree<String, Integer> integerIntegerMySubBPlusTree = new MySubBPlusTree<String, Integer>();
//        Random random = new Random();
//        ArrayList<Tuple2<String,Integer>> list = new ArrayList<Tuple2<String, Integer>>();
//       List<Integer> list2 = new ArrayList<Integer>();
//        LinkedList<Integer> integers = new LinkedList<Integer>();
//        integers.clear();
//
//        long time1 = System.nanoTime();
//        for (int i = 0;i<10000;i++){
//            integerIntegerMySubBPlusTree.insert("tuple:"+i,i,10l);
//        }
//        long time2 = System.nanoTime();
//        System.out.println("MyB+树插入耗时: \t" + (time2 - time1));
//
//        long time3 = System.nanoTime();
//        for (int i = 0;i<10000;i++){
//            list2.add(i);
//        }
//        long time4 = System.nanoTime();
//        System.out.println("数组插入耗时: \t" + (time4 - time3));
//
//        long time5 = System.nanoTime();
//        for (int i = 0;i<10000;i++){
//            List<String> range = integerIntegerMySubBPlusTree.findRange(i, i, 0l);
//        }
//        long time6 = System.nanoTime();
//        System.out.println("MyB+查询耗时: \t" + (time6 - time5)+"\n\t总耗时：\t"+(time6 - time5+time2 - time1));
//
//        long time7 = System.nanoTime();
//        for (int i = 0;i<10000;i++){
//            for (Integer e:list2){
//                if (e ==i){
//                    break;
//                }
//            }
//        }
//        long time8 = System.nanoTime();
//        System.out.println("数组查询耗时: \t" + (time8 - time7)+"\n\t总耗时：\t"+(time8 - time7+time4 - time3));
//        System.out.println();
//
//
//
//    }

    public static void main(String[] args) {
        MySubBPlusTree<String, Integer> integerIntegerMySubBPlusTree = new MySubBPlusTree<String, Integer>();
        for (int i = 0;i<10000;i++){
            integerIntegerMySubBPlusTree.insert("tuple:"+i,i,new Integer(i).longValue());
        }

        System.out.println(integerIntegerMySubBPlusTree.getMaxTimestamp());
        System.out.println(integerIntegerMySubBPlusTree.getMinTimestamp());

        MySubBPlusTree<String, Integer>.LinkListNode first = integerIntegerMySubBPlusTree.getFirst();
        for(int i = 0;i<=100;i++){
            first=first.next;
            System.out.println("item： " + first.item + " key： " + first.key + " timestamp： " + first.timestamp );
        }

        integerIntegerMySubBPlusTree.clear();
        System.out.println(integerIntegerMySubBPlusTree.getFirst());
        System.out.println(integerIntegerMySubBPlusTree.getLenght());
    }

    @Test
    public void BTreeListTest(){
        ArrayList<Tuple3<String, Integer, Long>> checkpointList = new ArrayList<Tuple3<String, Integer, Long>>();
        for (int i = 0;i<=1000;i++){
            checkpointList.add(new Tuple3<String, Integer, Long>("tuple:"+i,i,i*100L));
        }

        MySubBPlusTree<String, Integer> myBPlusTree = new MySubBPlusTree<String, Integer>(checkpointList, 3);

        List<Tuple3<String, Integer, Long>> list = myBPlusTree.getList();
        for (Tuple3<String, Integer, Long> t:
                list) {
            System.out.println("item: "+t.f0+" key: "+t.f1+" timestamp: "+t.f2);
        }
        System.out.println("最小时间: "+myBPlusTree.getMinTimestamp());
        System.out.println("最大时间: "+myBPlusTree.getMaxTimestamp());
        System.out.println("长度: "+myBPlusTree.getLenght());

        List<String> rangeWithoutTime = myBPlusTree.findRange(30, 30,10000L);
        for (String s:
             rangeWithoutTime) {
            System.out.println(s);

        }
    }


    @Test
    public void findRangeTest(){
        MySubBPlusTree<String, Integer> myBPlusTree = new MySubBPlusTree<String, Integer>( 3);
        for (int i = 0;i< 10000;i++){
            myBPlusTree.insert("item:"+i,i,i*100L);
        }

        List<String> range = myBPlusTree.findRange(10, 150, 10000L);
        for (String s:
             range) {
            System.out.println(s);

        }
    }

    @Test
    public void changeclassTest(){
        double a1 = 17.745;
        double a2 = 3.1264;
        int i1 = 7;
        int i2 = 2;
        System.out.println(a1/a2);
        System.out.println((int)(a1/a2));
        System.out.println(i1/i2);
    }

}
