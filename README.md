DELI: a Secondary Index for HBase/NoSQLs 
======

Introduction
------
``DELI`` is a secondary index support for NoSQL systems. It currently supports global indexing and is applicable for HBase alike NoSQL systems where write performance is optimized through a LSM tree structure [[link](http://en.wikipedia.org/wiki/Log-structured_merge-tree)]. 

DELI stands for "*DEferred Lightweight Indexing*", which is its uniqueness in design. The deferred indexing naturally extends the write-optimization principle of LSM trees (that is, all writes must be append-only) and works very well with HBase, Cassandra, etc.


Demo
---

1. Install ``docker`` on your machine; [[link](https://www.docker.com)]  
2. Launch the ``docker`` container running ``DELI`` image.  

    ```bash
    sudo docker run -i -t tristartom/deli-hadoop-hbase-ubuntu /bin/bash 
    ```
3. Inside the container's bash, run the following to demonstrate a DELI client program.  
    ```bash
    #step 0: first start hdfs                       
    source ~/.profile
    
    #step 1: first start hdfs                       
    cd ~/app/hadoop-2.6.0 
    bin/hdfs namenode -format 
    sbin/start-dfs.sh  
    #optional: verify hdfs runs successfully
    grep -w "FATAL\|ERROR" logs/*

    #step 2: then run hbase                         
    cd ~/app/hbase-0.99.2 
    bin/start-hbase.sh  
    #optional: shell
    bin/hbase shell
    #optional: verify hdfs runs successfully
    grep -w "FATAL\|ERROR" logs/*

    #step 3: run deli demo program
    cd ~/app/deli
    ant #compile the deli client
    ./tt_sh/run.sh #demonstrate data can be accessed through getByIndex interface.
    ```

If you observe ``Result is key1`` by the end of printout, it means the demo runs successfully. The demo source code can be found in ``~/app/deli/src/tthbase/client/Demo.java``

Reference
---

"Deferred Lightweight Indexing for Log-Structured Key-Value Stores", Yuzhe Tang, Arun Iyengar, Wei Tan, Liana Fong, Ling Liu, in Proceedings of the 15th IEEE/ACM International Symposium on Cluster, Cloud and Grid Computing (CCGrid 2015), Shenzhen, Guangdong, China, May 2015, [[pdf](http://tristartom.github.io/docs/ccgrid15.pdf)]
