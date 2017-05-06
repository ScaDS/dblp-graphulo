#!/bin/bash

cd ~/installs
echo ------------------------------STOP ACCUMULO------------------------------
~/installs/accumulo-1.8.1/bin/stop-all.sh

echo ------------------------------STOP ZOOKEEPER------------------------------
~/installs/zookeeper-3.4.10/bin/zkServer.sh stop

echo ------------------------------STOP HADOOP------------------------------
~/installs/hadoop-2.7.3/sbin/stop-yarn.sh
~/installs/hadoop-2.7.3/sbin/stop-dfs.sh