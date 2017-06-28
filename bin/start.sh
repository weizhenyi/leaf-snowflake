#!/bin/sh


if [ -e  ~/.bashrc ]
then
    source ~/.bashrc
fi

if [ -e  ~/.bash_profile ]
then
    source ~/.bash_profile
fi

if [ "x$JAVA_HOME" != "x" ]
then
    echo "JAVA_HOME has been set "
else
    echo "JAVA_HOME has not been set"
    exit 1
fi
echo "JAVA_HOME =" $JAVA_HOME

if [ "x$LEAF_HOME" != "x" ]
then
    echo "LEAF_HOME has been set "
else
    echo "LEAF_HOME has not been set "
    exit 1
fi
echo "LEAF_HOME =" $LEAF_HOME

if [ "x$LEAF_CONF_DIR" != "x" ]
then
    echo "LEAF_CONF_DIR has been set "
else
    echo "LEAF_CONF_DIR has not been set "
fi
echo "LEAF_CONF_DIR =" $LEAF_CONF_DIR



export PATH=$JAVA_HOME/bin:$LEAF_HOME/bin:$PATH


which java

if [ $? -eq 0 ]
then
    echo "Find java"
else
    echo "No java, please install java firstly !!!"
    exit 1
fi

function startLEAF()
{
	arg=$1
	echo "start leaf"
	nohup $LEAF_HOME/bin/leaf.py $arg >/dev/null 2>&1 &
	sleep 4
	rm -rf nohup
	ps -ef|grep leafServer
}

startLEAF "start"

echo "Successfully  start leaf daemon...."
