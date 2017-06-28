Leaf-snowflake
=============

Last Modified: 2017-06-28


Introduction
============
    Leaf-snowflake is a distributed ID maker coded by weizhenyi.
    Based on http://tech.meituan.com/MT_Leaf.html ,leaf-snowflake.

Steps:
============
1. mvn package
2. Set ${LEAF_HOME} environment variable
3. Config leaf.yaml at ${LEAF_HOME}/conf
4. Exec ${LEAF_HOME}/bin/start.sh
5. Communicate to the RPC server and get the uniqiue , monotonic increase ID like startClient() in /rpc/rpcClient.java shows,just for test!


# Author
weizhenyi
Github: https://github.com/weizhenyi



# Getting help
Email: zhenyiwei1986@126.com
QQ:63215516