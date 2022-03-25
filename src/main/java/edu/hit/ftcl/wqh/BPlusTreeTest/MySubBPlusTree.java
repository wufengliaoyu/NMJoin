package edu.hit.ftcl.wqh.BPlusTreeTest;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

public class MySubBPlusTree<T,K extends Comparable<K>> {
    //B+树的阶
    private Integer bTreeOrder;


    //当前B+树中存储的元组的数量
    private int lenght = 0;
    //底层有序链表的头节点
    private LinkListNode first;
    //底层有序链表的尾节点
    private LinkListNode last;

    //当前B+树存储的元组的最小以及最大的时间戳
    private Long minTimestamp;
    private Long maxTimestamp;

    //根节点
    private Node root;

    //树高，没什么用，可删除
    int hight=0;

    /**
     * 无参构造方法，默认阶为3，内部调用有参构造方法
     */
    public MySubBPlusTree() {
        this(3);
    }

    /**
     * 有参构造方法，可以设置B+树的阶，并进行相关的初始化
     * @param bTreeOrder B+树的阶
     */
    public MySubBPlusTree(Integer bTreeOrder) {
        this.bTreeOrder = bTreeOrder;
        this.root = new LeafNode();

        first = new LinkListNode(null,null,null,null,null);
        last = new LinkListNode(null,null,null,null,null);

        first.next = last;
        last.prev = first;

        minTimestamp = Long.MAX_VALUE;
        maxTimestamp = Long.MIN_VALUE;
    }

    /**
     * 利用一个 List<Tuple3<T,K,Long>> 的列表构建B+树，在读写检查点时会被调用
     * @param list 用于构建B+树的列表
     * @param bTreeOrder B+树的阶
     */
    public MySubBPlusTree(List<Tuple3<T,K,Long>> list , Integer bTreeOrder){
        //调用构造器指定B+树的阶
        this(bTreeOrder);

        //将List中的所有元组插入到当前B+树中
        for (Tuple3<T,K,Long> t : list){
            insert(t.f0,t.f1,t.f2);
        }

    }

    /**
     * 将底层链表作为一个 List 返回
     * @return 存储了所有底层链表元素的List
     */
    public List<Tuple3<T,K,Long>> getList(){
        List<Tuple3<T,K,Long>> resultList = new ArrayList<Tuple3<T, K, Long>>();
        LinkListNode next = first.next;
        while (next.next!=null){
            resultList.add(new Tuple3<T, K, Long>(next.item,next.key,next.timestamp));
            next=next.next;
        }
        return resultList;
    }

    /**
     * 获取当前B+树底层存储所有数据的链表的头部节点
     * @return 当前B+树底层存储所有数据的链表的头部节点
     */
    public LinkListNode getFirst() {
        return first;
    }

    /**
     * 获取存储数据的个数
     * @return 当前B+树中存储的元组的个数
     */
    public int getLenght() {
        return lenght;
    }

    /**
     * 获取当前B+树所有存储的元组中的最小时间戳
     * @return 当前B+树所有存储的元组中的最小时间戳
     */
    public Long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * 获取当前B+树所有存储的元组中的最大时间戳
     * @return 当前B+树所有存储的元组中的最大时间戳
     */
    public Long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * 范围查找
     * @param minKey 范围下界
     * @param maxKey 范围上界
     * @param timestamp 所要查找的最小时间戳
     * @return 所有满足时间条件及范围的结果所构成的链表
     */
    public List<T> findRange(K minKey,K maxKey,Long timestamp){

        //如果要查询的最小键值大于最大键值，则返回null
        if (minKey.compareTo(maxKey)>0){
            return null;
        }

        return this.root.findRange(minKey,maxKey,timestamp);
    }

    /**
     * 不考虑时间的范围查找（用于当前B+树的所有元组均满足时间要求的情况）
     * @param minKey 范围下界
     * @param maxKey 范围上界
     * @return 所有满足范围的结果所构成的链表
     */
    public List<T> findRangeWithoutTime(K minKey,K maxKey){

        //如果要查询的最小键值大于最大键值，则返回null
        if (minKey.compareTo(maxKey)>0){
            return null;
        }

        return this.root.findRangeWithoutTime(minKey,maxKey);
    }

    /**
     * 插入
     * @param tuple 插入的元组原始数据
     * @param key 插入的元组键值
     * @param timestamp 插入的元组时间戳
     */
    public void insert(T tuple,K key , Long timestamp){

        if(key == null){
            return;
        }

        //更新最小及最大时间戳
        if (timestamp < minTimestamp){
            minTimestamp = timestamp;
        }
        if (timestamp > maxTimestamp){
            maxTimestamp = timestamp;
        }

        Node insertNode = this.root.insert(tuple, key, timestamp,0,null);
        if(insertNode != null){
            this.root = insertNode;
            hight++;
        }
    }

    /**
     * 清除该子树，删除其中的所有指针
     */
    public void clear(){

        //从根节点开始删除所有的索引结构
        this.root.clear();

        //删除底层链表
        LinkListNode e = first;
        while(e != null){
            LinkListNode next = e.next;
            e.discard();
            e = next;
        }
        first = null;
        last = null;

        //删除相关的属性
        this.maxTimestamp = null;
        this.minTimestamp = null;
        this.lenght = 0;
        this.hight = 0;
        this.bTreeOrder = null;

    }

    /**
     * 内部节点以及叶子节点的公共父类
     *
     */
    abstract class Node{
        protected Node toParent; //指向父节点的指针
        protected int size;   //叶子节点以及内部非叶子节点所含键值的数量
        protected ArrayList<K> keys;  //保存叶子节点以及内部非叶子节点所含键值

        /**
         * 范围查找
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @param timestamp 所要查找的最小时间戳
         * @return 所有满足时间条件及范围的结果所构成的链表
         */
        abstract List<T> findRange(K minKey,K maxKey,Long timestamp);

        /**
         * 不考虑时间的范围查找（用于当前B+树的所有元组均满足时间要求的情况）
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @return 所有满足范围的结果所构成的链表
         */
        abstract List<T> findRangeWithoutTime(K minKey,K maxKey);

        /**
         * 插入一个元组
         * @param tuple 插入的实际元组
         * @param key 插入元组的键值
         * @param timestamp 插入元组的时间戳
         * @param position 调用当前方法的父节点的插入位置
         * @param parent 调用当前方法的父节点
         * @return
         */
        abstract Node insert(T tuple,K key,Long timestamp ,int position,InnerNode parent);

        /**
         * 清空该节点以及所有的子节点
         */
        abstract void clear();

    }

    /**
     * 内部节点（非叶子）
     *
     */
    class InnerNode extends Node{

        //保存指向当前内部节点所拥有的所有子节点的指针
        private ArrayList<Node> childs;

        public InnerNode() {
            this.size = 0;
            this.childs = new ArrayList<Node>();
            this.keys = new ArrayList<K>();
        }

        /**
         * 范围查找
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @param timestamp 所要查找的最小时间戳
         * @return 所有满足时间条件及范围的结果所构成的链表
         */
        List<T> findRange(K minKey, K maxKey, Long timestamp) {

            //当所要查找的最小值都比当前节点已经存储的最大值大时，查询结果为空
            if(minKey.compareTo(this.keys.get(this.keys.size()-1))>0){
                return null;
            }

            //destIndex为当前要查找的minKey在子节点当中的存储位置
            int destIndex;

            //二分法查找要进行查找的子节点
            int minIndex = 0;
            int maxIndex = this.childs.size() - 1;
            int middle;

            while(minIndex < maxIndex-1){
                middle = ( minIndex + maxIndex )/2;
                if (minKey.compareTo(this.keys.get(middle))>0){//当前要插入的键值大于存储键值的中间位置的键值
                    minIndex = middle;
                }else{
                    maxIndex = middle;
                }
            }

            if (minKey.compareTo(this.keys.get(minIndex))<=0){
                destIndex = minIndex;
            }else {
                destIndex = maxIndex;
            }

            //找到下一个要搜索的子节点
            Node nextSearchNode = this.childs.get(destIndex);

            //返回子节点的查询结果
            return nextSearchNode.findRange(minKey,maxKey,timestamp);
        }

        /**
         * 不考虑时间的范围查找（用于当前B+树的所有元组均满足时间要求的情况）
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @return 所有满足范围的结果所构成的链表
         */
        List<T> findRangeWithoutTime(K minKey, K maxKey) {
            //当所要查找的最小值都比当前节点已经存储的最大值大时，查询结果为空
            if(minKey.compareTo(this.keys.get(this.keys.size()-1))>0){
                return null;
            }

            //destIndex为当前要查找的minKey在子节点当中的存储位置
            int destIndex;

            //二分法查找要进行查找的子节点
            int minIndex = 0;
            int maxIndex = this.childs.size() - 1;
            int middle;

            while(minIndex < maxIndex-1){
                middle = ( minIndex + maxIndex )/2;
                if (minKey.compareTo(this.keys.get(middle))>0){//当前要插入的键值大于存储键值的中间位置的键值
                    minIndex = middle;
                }else{
                    maxIndex = middle;
                }
            }

            if (minKey.compareTo(this.keys.get(minIndex))<=0){
                destIndex = minIndex;
            }else {
                destIndex = maxIndex;
            }

            //找到下一个要搜索的子节点
            Node nextSearchNode = this.childs.get(destIndex);

            //返回子节点的查询结果
            return nextSearchNode.findRangeWithoutTime(minKey,maxKey);
        }

        /**
         * 内部非叶子节点的数据插入，此方法会递归调用
         * @param tuple 插入的实际元组
         * @param key 插入元组的键值
         * @param timestamp 插入元组的时间戳
         * @param position 调用当前方法的父节点的插入位置
         * @param parent 调用当前方法的父节点
         * @return 如果产生了新的根节点，则返回该根节点，否则返回null
         */
        Node insert(T tuple, K key, Long timestamp ,int position,InnerNode parent) {

            Node nextInsertNode;//接下来要进行插入操作的子节点
            //destIndex为当前要插入的位置
            int destIndex;

            if (key.compareTo(this.keys.get(this.keys.size()-1))>0){//当当前要插入的键值比当前节点已存储的最大的键值都大时

                //更新当前节点所存储键值的最大值
                this.keys.set(this.keys.size()-1,key);

                //下一个要进行插入的节点是最后的一个子节点
                destIndex = this.keys.size()-1;
                nextInsertNode = this.childs.get(destIndex);

            }else{//要插入的键值不大于已存储的最大键值
                //使用二分搜索法搜索插入的位置
                int minIndex = 0;
                int maxIndex = this.childs.size() - 1;
                int middle;

                while(minIndex < maxIndex-1){
                    middle = ( minIndex + maxIndex )/2;
                    if (key.compareTo(this.keys.get(middle))>0){//当前要插入的键值大于存储键值的中间位置的键值
                        minIndex = middle;
                    }else{
                        maxIndex = middle;
                    }
                }


                if (key.compareTo(this.keys.get(minIndex))<=0){
                    destIndex = minIndex;
                }else {
                    destIndex = maxIndex;
                }

                //获取要进行插入的子节点
                nextInsertNode = this.childs.get(destIndex);

            }

            //调用子节点的插入方法
            nextInsertNode.insert(tuple,key,timestamp,destIndex,this);

            //判断当前内部节点需不需要分裂
            if (this.keys.size()>bTreeOrder){ //当前内部节点保存的键值数量大于B+树的阶，需要分裂

                InnerNode newInnerNode = new InnerNode(); //新建一个内部节点，作为当前节点左侧的节点
                int leftSize = this.keys.size()/2; //分裂之后左侧节点的大小
                int rightSize = this.keys.size() - leftSize;
                for (int i = 0;i<leftSize;i++){
                    //将当前内部节点左半部分的存储数据保存到新建的节点中
                    newInnerNode.keys.add(this.keys.get(0));
                    newInnerNode.childs.add(this.childs.get(0));
                    this.keys.remove(0);
                    this.childs.remove(0);
                }

                //更新大小
                newInnerNode.size = leftSize;
                this.size = rightSize;

                if (parent==null){//当前叶子节点没有父节点，则新建一个返回

                    //新建一个父节点
                    InnerNode parentInnerNode = new InnerNode();
                    //将新建的节点以及当前节点插入到新建的内部节点当中
                    parentInnerNode.keys.add(newInnerNode.keys.get(newInnerNode.keys.size()-1));
                    parentInnerNode.childs.add(newInnerNode);
                    parentInnerNode.keys.add(this.keys.get(this.keys.size()-1));
                    parentInnerNode.childs.add(this);

                    //更新两个节点的父指针
                    newInnerNode.toParent = parentInnerNode;
                    this.toParent = parentInnerNode;


                    parentInnerNode.size = 2;
                    //返回新建的父节点
                    return parentInnerNode;

                }else {//当前叶子节点有父节点，则将当前节点插入到父节点中

                    //插入到调用此方法的内部节点的位置
                    parent.keys.add(position,newInnerNode.keys.get(newInnerNode.keys.size()-1));
                    parent.childs.add(position,newInnerNode);

                    newInnerNode.toParent = parent;

                    //父节点的size加一
                    parent.size++;

                    return null;
                }

            }else {//当前节点不需要分裂
                return null;
            }


        }

        /**
         * 清空该内部非叶子节点
         */
        void clear() {
            //清空所有的子节点
            for(Node child : childs){
                child.clear();
            }

            //清空该节点
            this.childs.clear();
            this.keys.clear();
            this.toParent = null;
            this.size=0;
        }
    }

    /**
     * 叶子节点
     *
     */
    class LeafNode extends Node{

        //存储其指向的底层有序链表
        ArrayList<LinkListNode> childs;

        public LeafNode() {
            this.size=0;
            this.childs = new ArrayList<LinkListNode>();
            this.keys = new ArrayList<K>();

        }

        /**
         * 范围查找
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @param timestamp 所要查找的最小时间戳
         * @return 所有满足时间条件及范围的结果所构成的链表
         */
        List<T> findRange(K minKey, K maxKey, Long timestamp) {

            //返回的满足连接条件及时间要求的结果列表
            List<T> resultList = new ArrayList<T>();

            //获取当前叶子节点指向的第一个（即key值最小）的底层链表节点的指针
            LinkListNode currentLinkListNode = this.childs.get(0);

            //跳过前面所有key小于minKey的节点,此后的节点key均大于等于minKey。(当前节点不能为尾节点)
            while (currentLinkListNode.next!=null && currentLinkListNode.key.compareTo(minKey)<0){
                currentLinkListNode=currentLinkListNode.next;
            }

            //找寻所有key大于等于minKey的节点中，key小于等于maxKey的节点。(当前节点不能为尾节点)
            while (currentLinkListNode.next!=null && currentLinkListNode.key.compareTo(maxKey)<=0){
                //如果满足时间要求（即在指定的时间戳之后到达），则将当前节点当中存储的数据放入返回链表中
                if (currentLinkListNode.timestamp>=timestamp){
                    resultList.add(currentLinkListNode.item);
                }
                //这个地方原本是个BUG，找了好久终于找到了，不管时间戳满不满足要求，都要把指针后移一位
                currentLinkListNode = currentLinkListNode.next;

            }

            return resultList;
        }

        /**
         * 不考虑时间的范围查找（用于当前B+树的所有元组均满足时间要求的情况）
         * @param minKey 范围下界
         * @param maxKey 范围上界
         * @return 所有满足范围的结果所构成的链表
         */
        List<T> findRangeWithoutTime(K minKey, K maxKey) {
            //返回的满足连接条件及时间要求的结果列表
            List<T> resultList = new ArrayList<T>();

            //获取当前叶子节点指向的第一个（即key值最小）的底层链表节点的指针
            LinkListNode currentLinkListNode = this.childs.get(0);

            //跳过前面所有key小于minKey的节点,此后的节点key均大于等于minKey。(当前节点不能为尾节点)
            while (currentLinkListNode.next!=null && currentLinkListNode.key.compareTo(minKey)<0){
                currentLinkListNode=currentLinkListNode.next;
            }

            //找寻所有key大于等于minKey的节点中，key小于等于maxKey的节点。(当前节点不能为尾节点)
            while (currentLinkListNode.next!=null && currentLinkListNode.key.compareTo(maxKey)<=0){

                //不考虑时间限制，直接将满足条件的元组放入到返回列表中
                resultList.add(currentLinkListNode.item);
                currentLinkListNode = currentLinkListNode.next;


            }

            return resultList;
        }

        /**
         * 向叶子节点插入数据，叶子节点用 ArrayList 保存指向最低层实际存储数据节点 LinkListNode 的指针
         * 最低层实际存储数据节点 LinkListNode 中的数据根据 key 值升序排列
         * @param tuple 插入的实际元组
         * @param key 插入元组的键值
         * @param timestamp 插入元组的时间戳
         * @param position 调用当前方法的父节点的插入位置
         * @param parent 调用当前方法的父节点
         * @return 若当前叶子节点没有父节点，则返回 null ，否则，新建一个内部节点 InnerNode 作为当前叶子节点的父节点返回
         */
        Node insert(T tuple, K key, Long timestamp ,int position,InnerNode parent) {

            //构建底层存储列表的数据节点
            LinkListNode newLinkListNode = new LinkListNode(tuple, key, timestamp, null, null);

            //当向整个B+树中第一次添加数据时
            if (lenght == 0){

                //添加节点以及键值
                keys.add(key);
                childs.add(newLinkListNode);

                //链接底部链表
                first.next=newLinkListNode;
                newLinkListNode.prev=first;

                last.prev = newLinkListNode;
                newLinkListNode.next=last;

                //更新当前叶子节点的大小以及整个B+树的长度
                this.size++;
                lenght++;

                return null;
            }

            //当不是向整个B+树中第一次添加数据时

            if (key.compareTo(this.keys.get(this.keys.size()-1))>0){//当当前要插入的键值比当前叶子节点已经存储的最大键值还大时
                //保存原来的最大键值的元组
                LinkListNode lastLinkListNode = this.childs.get(this.keys.size()-1);
                //插入新的节点
                this.keys.add(key);
                this.childs.add(newLinkListNode);
                //链接底层链表
                lastLinkListNode.next.prev = newLinkListNode;
                newLinkListNode.next = lastLinkListNode.next;
                lastLinkListNode.next = newLinkListNode;
                newLinkListNode.prev = lastLinkListNode;

                //更新当前叶子节点的大小以及整个B+树的长度
                this.size++;
                lenght++;

            }else{
                //使用二分搜索法搜索插入的位置
                int minIndex = 0;
                int maxIndex = this.childs.size() - 1;
                int middle;

                while(minIndex < maxIndex-1){
                    middle = ( minIndex + maxIndex )/2;
                    if (key.compareTo(this.keys.get(middle))>0){//当前要插入的键值大于存储键值的中间位置的键值
                        minIndex = middle;
                    }else{
                        maxIndex = middle;
                    }
                }

                //destIndex为当前要插入的位置,目前其中保存着要插入节点的后继节点
                int destIndex;
                if (key.compareTo(this.keys.get(minIndex))<=0){
                    destIndex = minIndex;
                }else {
                    destIndex = maxIndex;
                }

                //保存当前要插入节点的后继节点
                LinkListNode lastLinkListNode = this.childs.get(destIndex);

                //插入新的节点
                this.keys.add(destIndex,key);
                this.childs.add(destIndex,newLinkListNode);

                //链接底层链表
                lastLinkListNode.prev.next = newLinkListNode;
                newLinkListNode.prev = lastLinkListNode.prev;
                lastLinkListNode.prev = newLinkListNode;
                newLinkListNode.next = lastLinkListNode;

                //更新当前叶子节点的大小以及整个B+树的长度
                this.size++;
                lenght++;

            }


            //判断当前叶子节点需不需要分裂
            if (this.keys.size()>bTreeOrder){ //当前叶子节点保存的键值数量大于B+树的阶，需要分裂

                LeafNode newLeafNode = new LeafNode(); //新建一个叶子节点，作为当前节点左侧的节点
                int leftSize = this.keys.size()/2; //分裂之后左侧节点的大小
                int rightSize = this.keys.size() - leftSize;
                for (int i = 0;i<leftSize;i++){
                    //将当前叶子节点左半部分的存储数据保存到新建的节点中
                    newLeafNode.keys.add(this.keys.get(0));
                    newLeafNode.childs.add(this.childs.get(0));
                    this.keys.remove(0);
                    this.childs.remove(0);
                }

                //更新大小
                newLeafNode.size = leftSize;
                this.size = rightSize;

                if (parent==null){//当前叶子节点没有父节点，则新建一个返回

                    //新建一个父节点
                    InnerNode parentInnerNode = new InnerNode();
                    //将新建的节点以及当前节点插入到新建的内部节点当中
                    parentInnerNode.keys.add(newLeafNode.keys.get(newLeafNode.keys.size()-1));
                    parentInnerNode.childs.add(newLeafNode);
                    parentInnerNode.keys.add(this.keys.get(this.keys.size()-1));
                    parentInnerNode.childs.add(this);

                    //更新两个节点的父指针
                    newLeafNode.toParent = parentInnerNode;
                    this.toParent = parentInnerNode;


                    parentInnerNode.size = 2;
                    //返回新建的父节点
                    return parentInnerNode;

                }else {//当前叶子节点有父节点，则将当前节点插入到父节点中

                    //插入到调用此方法的内部节点的位置
                    parent.keys.add(position,newLeafNode.keys.get(newLeafNode.keys.size()-1));
                    parent.childs.add(position,newLeafNode);

                    newLeafNode.toParent = parent;

                    //父节点的size加一
                    parent.size++;
                    return null;
                }

            }else {//当前节点不需要分裂
                return null;
            }


        }


        /**
         * 清空该叶子节点
         */
        void clear() {
            this.childs.clear();
            this.keys.clear();
            this.toParent = null;
            this.size = 0;
        }


    }

    /**
     * 底层的有序链表的节点
     */
    class LinkListNode{
        T item;
        K key;
        Long timestamp;
        LinkListNode next;
        LinkListNode prev;

        public LinkListNode(T item, K key, Long timestamp, LinkListNode next, LinkListNode prev) {
            this.item = item;
            this.key = key;
            this.timestamp = timestamp;
            this.next = next;
            this.prev = prev;
        }

        //删除该节点
        public void discard(){
            this.item = null;
            this.key = null;
            this.timestamp = null;
            this.next = null;
            this.prev = null;
        }

    }



}
