#!/bin/bash

#Downloads and builds graphulo, inits a default config and test wrapper script

cd ~/installs
touch test.sh
echo "#!/bin/bash" >> test.sh
echo "cd graphulo" >> test.sh
echo "mvn test -DTEST_CONFIG=./example.conf -Dtest=TableMultExample" >> test.sh
chmod +x test.sh
git clone https://github.com/Accla/graphulo.git
sudo apt-get install maven

cd graphulo
rm example.conf
touch example.conf
echo "accumulo.it.cluster.standalone.admin.principal=root" >> example.conf
echo "accumulo.it.cluster.standalone.admin.password=acc" >> example.conf
echo "accumulo.it.cluster.standalone.zookeepers=localhost:2181" >> example.conf
echo "accumulo.it.cluster.standalone.instance.name=bdp" >> example.conf
mvn install -DskipTests
./deploy.sh

