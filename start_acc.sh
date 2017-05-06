#!/bin/bash

cd ~/installs
echo ------------------------------START HADOOP------------------------------
~/installs/hadoop-2.7.3/sbin/start-dfs.sh

echo ------------------------------START ZOOKEEPER------------------------------
~/installs/zookeeper-3.4.10/bin/zkServer.sh start

echo ------------------------------START ACCUMULO------------------------------
~/installs/accumulo-1.8.1/bin/start-all.sh