#!/bin/bash

cd ~/installs/dblp-graphulo
./gradlew assemble
cp build/libs/dblp-graphulo-1.0-SNAPSHOT.jar "$ACCUMULO_HOME/lib/ext"
echo "Installed dblp-graphulo on Accumulo"