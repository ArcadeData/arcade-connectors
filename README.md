[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/ArcadeAnalytics/arcade-connectors.svg?branch=master)](https://travis-ci.org/ArcadeAnalytics/arcade-connectors)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.arcadeanalytics/arcade-connectors-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.arcadeanalytics/arcade-connectors-parent)
[![security status](https://www.meterian.com/badge/gh/ArcadeAnalytics/arcade-connectors/security)](https://www.meterian.com/report/gh/ArcadeAnalytics/arcade-connectors)
[![stability status](https://www.meterian.com/badge/gh/ArcadeAnalytics/arcade-connectors/stability)](https://www.meterian.com/report/gh/ArcadeAnalytics/arcade-connectors)
[![Javadocs](https://javadoc.io/badge/com.arcadeanalytics/arcade-connectors-base.svg)](https://javadoc.io/doc/com.arcadeanalytics/arcade-connectors-base)

[WIP]

# Arcade Connectors 

Arcade Conecotors are a set of components for fetching data and metadata from different data stores.

Supported data stores are

- OrientDB 2.x and 3.x 
- Neo4j
- Memgraph
- Gremlin end points: OrientDB, JanusGraph, Amazon Neptune, Azure CosmosDB
- RDBMS: PostgreSQL, Mysql, MariaDB

The base module defines the interfaces and implements the SSH tunneling that could be used with each of the specialized modules.
Moreover provides the factories to create connectors. 

## Build and test

To perform integrations, tests the projects uses [Testcontainers](https://www.testcontainers.org/) and needs [Docker](https://www.docker.com/) installed.

To build and test just run:
    
    mvn clean install

## Modules

The base module contains the definitions of the interfaces each connector should implements, the ssh-tunnel support and factories.

## Interfaces

- `DataSourceGraphDataProvider`: defines methods to fetch data and perform traverse on the graph
- `DataSourceGraphProvider`: defines method to extract the entire graph from the datasource (used to index the whole graph)
- `DataSourceMetadataProvider`: defines methods to extract metadata from the datasource, such as vertices and edge classes/labels
- `DataSourceTableDataProvider`: defines methods to provide tabular data used in charts

## Factory

There is a single, generified factory: `DataSourceProviderFactory`
To instantiate the factory for one of the provided interface:

    factory = DataSourceProviderFactory(DataSourceGraphDataProvider::class.java)

The factory will scan the `pluginsa` directory and loads each implementation on a different classloader.

