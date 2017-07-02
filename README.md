Leaf-snowflake
=============

Last Modified: 2017-06-28
    
## Introduction

Leaf-snowflake is a distributed ID maker.

Based on <http://tech.meituan.com/MT_Leaf.html> leaf-snowflake.

Steps:
============
1. mvn package
2. Set ${LEAF_HOME} environment variable
3. Config leaf.yaml at ${LEAF_HOME}/conf
4. Exec ${LEAF_HOME}/bin/start.sh
5. Communicate to the RPC server and get the unique , monotonic increase ID like startClient() in /rpc/rpcClient.java shows,just for test!

Commond line tools
============
1. List the content of a znode dir:

   /bin/leaf.py zktool list /leaf/server-forever

2. Read the content of a znode:

   /bin/leaf.py zktool read /leaf/server-forever/172.21.0.190:2182-0000000008


Throughput capacity test
============

Cost 191 seconds with 200W absolutely  monotonic increase Ids generated.

TPS: 1W+/sec for a single machine


# Author
weizhenyi
Github: https://github.com/weizhenyi



# Getting help
Email: zhenyiwei1986@126.com
QQ:632155186