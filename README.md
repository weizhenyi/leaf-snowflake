Leaf-snowflake
=============

Last Modified: 2017-06-28


Introduction
============
    leaf-snowflake is a Distributed ID maker coded by weizhenyi
based on http://tech.meituan.com/MT_Leaf.html Leaf-snowflake


Steps:
============
1. mvn package\n
2. Set LEAF_HOME\n
3. Config leaf.yaml at ${LEAF_HOME}/conf\n
4. Exec ${LEAF_HOME}/bin/start.sh\n
5. Communicate to the RPC server and get the uniqiue , monotonic increase ID like startClient() in /rpc/rpcClient.java shows,just for test!\n


# Author
weizhenyi
Github: https://github.com/weizhenyi



# Getting help
Email: zhenyiwei1986@126.com
QQ:63215516