#!/bin/sh


function killLeaf()
{
  ps -ef|grep $1|grep -v grep |awk '{print $2}' |xargs kill
	sleep 1
	ps -ef|grep $1

	echo "kill "$1
}

killLeaf "leafServer"
echo "Successfully stop leafServer"
