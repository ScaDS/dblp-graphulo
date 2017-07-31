# dblp-graphulo

Interface for the DBLP-Parser to save data into an Accumulo instance as a Graphulo sparse matrix.
Implementation of algorithms to calculate weakly and strongly connected components and analysis.
Written for the Big Data Praktikum at the University of Leipzig.

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

## Preparation

[Infrastructure](./src/main/java/de/alkern/infrastructure) contains classes to 
[connect](./src/main/java/de/alkern/infrastructure/connector) to Accumulo and an abstraction
for ["Graphulo-formed" Accumulo-Entries](./src/main/java/de/alkern/infrastructure/entry).

To use the DBLP-data, you can implement a DBLP-Processor, check [this](https://github.com/ScaDS/dblp-parser)
and [this](./src/main/java/de/alkern/author).

## Running

Calculate components (really slow) using [this](./src/main/java/de/alkern/connected_components). For analysis use 
[this](./src/main/java/de/alkern/connected_components/analysis).