# NMJoin
NMJoin, built on Flink, is a distributed stream window join system dedicated to solving the data skew problem in stream processing systems.
# Background
In the field of real-time analytics, stream joins are the basis for complex queries and greatly affect system performance. Similar to other big data problems, stream joins suffer from load imbalance, and a few join nodes carrying too much load can become the bottleneck. To solve this problem, in this paper, we propose an adaptive non-migrating load balancing method, which is mainly oriented to the stream window join problem. Our method performs load balancing by controlling the replication and forwarding of tuples instead of data migration. Compared to existing load balancing methods, our method can perform load balancing with very low additional overhead, and thus outperforms traditional methods in terms of latency and throughput. Based on our method, we develop a distributed stream window join system, NMJoin, which is built on Flink.
# System Architecture
NMJoin is similar to SplitJoin in that each joiner instance is responsible for storing and joining partial tuples of the two input streams, and each tuple is stored in only one joiner instance. However, unlike SplitJoin, NMJoin uses routing tables to specify the partitions of different tuples, instead of using broadcast to distribute tuples.

NMJoin consists of three main components: router, joiner, and coordinator.

`Router`. The router is responsible for ingesting tuples arriving at the system and partitioning them. When a tuple arrives at the router, the router constructs a store tuple for it and sends it to the corresponding store partition for storing, and constructs several join tuples for it and sends them to the corresponding join partitions for joining. There are two routing tables in each router instance for each input stream, i.e., the store routing table and the join routing table. In addition, when the system performs load balancing, the router receives the latest store routing table from the coordinator and updates its local join routing table based on the store routing table to perform load balancing.

`Joiner`. The joiner is responsible for performing the actual join operation. When a tuple arrives at a joiner instance, the joiner instance performs the store or join operation depending on the type of the tuple. Since NMJoin is oriented towards window join, we use the structure of the chained in-memory index to reduce the overhead of window tuples expiring. The chained in-memory index organizes all the stored tuples into several sub-indexes based on their timestamp, and the tuples are deleted with the granularity of the sub-indexes. In addition, to calculate the load balancing scheme, each joiner instance periodically uploads its local load information and related statistics to the coordinator.

`Coordinator`. The coordinator periodically collects load information and statistics uploaded by each joiner instance and then determines whether the system needs to perform load balancing. If the coordinator determines that the system needs to perform load balancing, the coordinator will recalculate a store routing table based on the statistics and then notify each router instance to get the latest store routing table.

# Install
NMJoin source code is maintained using Maven. Generate the excutable jar by running.

`mvn clean package`

# Usage
NMJoin is built on Flink and uses Kafka as the input stream adapter. Meanwhile, the components communicate with each other through Zookeeper. Before running NMJoin you need to make sure that Flink, Kafka and Zookeeper have been deployed in your cluster and that the relevant parameters have been configured in the configuration file. You can configure the relevant parameters required for NMJoin in the following file. For example, the system parallelism, the number of groups of Joiners, etc.

`CommonConfigParameters`

`NMJoinConfigParameters`

`CommonTestDataGenerateParameters`

 # Maintainers
 NMJoin is developed in Research Center of Fault-tolerant and Mobile Computing, Department of Computer Science and Technology, Harbin Institute of Technology, Harbin, China.
