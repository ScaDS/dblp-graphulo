#!/bin/bash

cd ~/installs
touch test.sh
echo mvn test -DTEST_CONFIG=./example.conf -Dtest=TableMultExample >> test.sh
git clone https://github.com/Accla/graphulo.git
sudo apt-get install maven
cd graphulo
mvn package -DskipTests
rm example.conf
touch example.conf
echo accumulo.it.cluster.standalone.admin.principal=username >> example.conf
echo accumulo.it.cluster.standalone.admin.password=acc >> example.conf
echo accumulo.it.cluster.standalone.zookeepers=localhost:2181 >> example.conf
echo accumulo.it.cluster.standalone.instance.name=bdp >> example.conf

