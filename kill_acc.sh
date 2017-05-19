#!/bin/bash

#Stops Accumulo, Zookeeper and Hadoop
#Removes logs

cd ~/installs
echo ------------------------------STOP ACCUMULO------------------------------
accumulo-1.8.1/bin/stop-all.sh
rm -rf accumulo-1.8.1/logs/*

echo ------------------------------STOP ZOOKEEPER------------------------------
zookeeper-3.4.10/bin/zkServer.sh stop

echo ------------------------------STOP HADOOP------------------------------
hadoop-2.7.3/sbin/stop-yarn.sh
hadoop-2.7.3/sbin/stop-dfs.sh
rm -rf hadoop-2.7.3/logs/*