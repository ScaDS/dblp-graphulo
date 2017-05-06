#!/bin/bash

#Source: https://www.digitalocean.com/community/tutorials/how-to-install-the-big-data-friendly-apache-accumulo-nosql-database-on-ubuntu-14-04
#installs Java, Hadoop, Zookeeper and Accumulo

echo ------------------------------INSTALL JAVA------------------------------
sudo apt-get update
sudo apt-get install openjdk-8-jdk
echo export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 >> ~/.bashrc

echo ------------------------------INSTALL SSH------------------------------
sudo apt-get install ssh rsync

echo ------------------------------DOWNLOAD FILES------------------------------
mkdir -p ~/downloads
cd ~/downloads
wget "http://www.eu.apache.org/dist/hadoop/common/stable/hadoop-2.7.3.tar.gz"
wget "http://www.eu.apache.org/dist/zookeeper/stable/zookeeper-3.4.10.tar.gz"
wget "http://www.eu.apache.org/dist/accumulo/1.8.1/accumulo-1.8.1-bin.tar.gz"
mkdir -p ~/installs
cd ~/installs

echo ------------------------------INSTALL HADOOP------------------------------
tar -xvzf ~/downloads/hadoop-2.7.3.tar.gz
touch ~/installs/hadoop-2.7.3/etc/hadoop/mapred-site.xml
echo "<?xml version="1.0"?>" >> ~/installs/hadoop-2.7.3/etc/hadoop/mapred-site.xml
echo "<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>" >> ~/installs/hadoop-2.7.3/etc/hadoop/mapred-site.xml
echo "<configuration>" >> ~/installs/hadoop-2.7.3/etc/hadoop/mapred-site.xml
echo "     <property>" >> ~/installs/hadoop-2.7.3/etc/hadoop/mapred-site.xml
echo "         <name>mapred.job.tracker</name>" >> ~/installs/hadoop-2.7.3/etc/hadoop/mapred-site.xml
echo "         <value>localhost:9001</value>" >> ~/installs/hadoop-2.7.3/etc/hadoop/mapred-site.xml
echo "     </property> >>" ~/installs/hadoop-2.7.3/etc/hadoop/mapred-site.xml
echo "</configuration>" >> ~/installs/hadoop-2.7.3/etc/hadoop/mapred-site.xml
cd ~/installs/hadoop-2.7.3/
~/installs/hadoop-2.7.3/bin/hdfs namenode -format

echo ------------------------------INSTALL ZOOKEEPER------------------------------
cd ~/installs
tar -xvzf ~/downloads/zookeeper-3.4.10.tar.gz
cp ~/installs/zookeeper-3.4.10/conf/zoo_sample.cfg ~/installs/zookeeper-3.4.10/conf/zoo.cfg

echo ------------------------------INSTALL ACCUMULO------------------------------
tar -xvzf ~/downloads/accumulo-1.8.1-bin.tar.gz
cp ~/installs/accumulo-1.8.1/conf/examples/512MB/standalone/* ~/installs/accumulo-1.8.1/conf/
echo export HADOOP_HOME=~/installs/hadoop-2.7.3/ >> ~/.bashrc
echo export ZOOKEEPER_HOME=~/installs/zookeeper-3.4.10/ >> ~/.bashrc

. ~/.bashrc
rm -rf ~/downloads

echo ------------------------------TODO------------------------------
echo ------------------------------
echo "sudo nano $JAVA_HOME/jre/lib/security/java.security"
echo "CHANGE LINE TO: securerandom.source=file:/dev/./urandom"
echo ------------------------------
echo "ssh-keygen -P ''"
echo "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys"
echo "ssh localhost (QUIT WITH EXIT)"
echo "ssh 0.0.0.0 (QUIT WITH EXIT)"
echo ------------------------------
echo "nano ~/installs/hadoop-2.7.3/etc/hadoop/hadoop-env.sh"
echo "CHANGE LINE TO: export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64"
echo "CHANGE LINE TO: export HADOOP_OPTS=\"$HADOOP_OPTS -XX:-PrintWarnings -Djava.net.preferIPv4Stack=true\""
echo "nano ~/installs/hadoop-2.7.3/etc/hadoop/core-site.xml"
echo "ADD XML-PROPERTY: fs.defaultFS=hdfs://localhost:9000"
echo "nano ~/installs/hadoop-2.7.3/etc/hadoop/hdfs-site.xml"
echo "ADD XML-PROPERTY: dfs.replication=1"
echo "ADD XML-PROPERTY: dfs.name.dir=hdfs_storage/name"
echo "ADD XML-PROPERTY: dfs.data.dir= hdfs_storage/data"
echo ------------------------------
echo "nano ~/installs/accumulo-1.8.1/conf/accumulo-env.sh"
echo "CHANGE LINE TO: export ACCUMULO_MONITOR_BIND_ALL=\"true\""
echo "SET JVM-SPACE HIGHER"
echo "nano ~/installs/accumulo-1.8.1/conf/accumulo-site.xml"
echo "CHANGE XML-PROPERTY: instance.secret=acc"
echo "CHANGE XML-PROPERTY: trace.token.property.password=acc"
echo "ADD XML-PROPERTY: instance.volumes=hdfs://localhost:9000/accumulo"
echo "INIT INSTANCE_NAME bdp PASSWORD acc: ~/Installs/accumulo-1.8.1/bin/accumulo init"
