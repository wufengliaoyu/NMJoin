# NMJoin
##Abstract
In the field of real-time analytics, stream joins are the basis for complex queries and greatly affect system performance. 
Similar to other big data problems, stream joins suffer from load imbalance, and a few join nodes carrying too much load can become the bottleneck.
To solve this problem, in this paper, we propose an adaptive non-migrating load balancing method, which is mainly oriented to the stream window join problem. 
Our method performs load balancing by controlling the replication and forwarding of tuples instead of data migration.
Compared to existing load balancing methods, our method can perform load balancing with very low additional overhead,  and thus outperforms traditional methods in terms of latency and throughput. 
Based on our method, we develop a distributed stream window join system, NMJoin, which is built on Flink. 


NMJoin is developed in Research Center of Fault-tolerant and Mobile Computing, Department of Computer Science and Technology, Harbin Institute of Technology, Harbin, China.
