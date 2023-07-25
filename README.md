# OpenRank for Neo4j GDS

This repository is an OpenRank Neo4j plugin implementation with Neo4j GDS library.

## Build & Test

You may use [Gradle](https://gradle.org/) 8 to build and test the project.

Run `gradle test` to run unit tests and `./gradlew clean build` to build the project, you will find the `openrank-neo4j-gds.jar` after build under `./build/libs`.

## Compatibility

This plugin uses [Neo4j Community Edition 5.6.0](https://neo4j.com/download-center/#community) and [Neo4j GDS(Graph Data Science) 2.4.2](https://github.com/neo4j/graph-data-science/releases/tag/2.4.2), feel free to build your own plugin to change the versions under [`build.gradle`](./build.gradle).

## Usage

This plugin uses Pregel framework provided by GDS so it only contains the computation part of the OpenRank, you can run the algorithm after build the in-memory graph like this:

`CALL xlab.pregel.openrank.write('graphName',{initValueProperty:'initValue',retentionFactorProperty:'retentionFactor',relationshipWeightProperty:'weight',tolerance:0.001,maxIterations:40,suffix:'_suffix'});`

- `graphName`: the in-memory graph name to run OpenRank.
- `intiValueProperty`: the init value property on nodes to indicate the initial value of each node.
- `retentionFactorProperty`: just like dumping factor in PageRank, retention factor is used to indicate how much a node's initial value used in the algorithm.
- `relationshipWeightProperty`: the property to indicate the weight of relationships in the graph.
- `tolerance`: the percision of the algorithm.
- `maxIterations`: the max iteration limit in case the algorithm not converge.
- `suffix`: used in write mode to append after the `open_rank` property, like timestamp.

## License

This repository is licensed under the GNU Public License version 3.0.
