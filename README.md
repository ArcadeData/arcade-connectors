[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.org/ArcadeAnalytics/arcade-connectors.svg?branch=master)](https://travis-ci.org/ArcadeAnalytics/arcade-connectors)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.arcadeanalytics/arcade-connectors-parent/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.arcadeanalytics/arcade-connectors-parent)

# Arcade Connectors 

Arcade Conecotors are a set of components for fetching data and metadata from different data stores.

Supported data stores are

- OrientDB 2.x and 3.x 
- Neo4j
- Memgraph
- Gremlin end points: OrientDB, JanusGraph, Amazone Neptune, Azure CosmosDB
- RDBMS: PostgreSQL, Mysql, MariaDB

The base module defines the interfaces and implements the SSH tunneling that could be used with each of the specialized modules.
Moreover provides the factories to create connectors. 

## Interfaces

## Factories

