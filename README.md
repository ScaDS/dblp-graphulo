# dblp-graphulo

Interface for the DBLP-Parser to save data into an Accumulo instance as a Graphulo sparse matrix.
Implementation of Algorithms to calculate weakly and strongly connected components

## Requirements

Maven, Gradle, Accumulo

## Installation

Tested on Ubuntu 16.04

* Run [install_acc.sh](./scripts/install_acc.sh) and execute the manual commands
* Run [graphulo.sh](./scripts/graphulo.sh)
* Download [DBLP-Parser](https://github.com/ScaDS/dblp-parser)
* Run `gradle build` in DBLP-Parser
* Run [build_and_copy.sh](./scripts/build_and_copy.sh)
* Download the DBLP data and add it to [resources](./src/main/resources)
