#!/bin/bash

cd ~/installs
echo ------------------------------START HADOOP------------------------------
hadoop-2.7.3/sbin/start-yarn.sh
hadoop-2.7.3/sbin/start-dfs.sh

echo ------------------------------START ZOOKEEPER------------------------------
zookeeper-3.4.10/bin/zkServer.sh start

echo ------------------------------START ACCUMULO------------------------------
accumulo-1.8.1/bin/accumulo init
accumulo-1.8.1/bin/start-all.sh