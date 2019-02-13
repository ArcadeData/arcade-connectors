/*-
 * #%L
 * Arcade Data
 * %%
 * Copyright (C) 2018 - 2019 ArcadeAnalytics
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.arcadeanalytics.provider.neo4j3

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.provider.*
import org.neo4j.driver.v1.StatementResult
import org.neo4j.driver.v1.types.Node
import org.neo4j.driver.v1.types.Relationship
import org.slf4j.LoggerFactory
import java.util.*

class Neo4jStatementResultMapper(private val dataSource: DataSourceInfo, private val maxTraversal: Int) {

    private fun countInOut(cyto: CytoData, rels: Set<Relationship>): CytoData {

        val nodeId = toNeo4jId(dataSource, cyto.data.id).toLong()

//        val inAndOutCountByEdgeType: MutableMap<String, Map<String, Int>> = rels.asSequence()
//                .filter { r -> r.endNodeId() == nodeId || r.startNodeId() == nodeId }
//                .groupBy { r -> if (r.endNodeId() == nodeId) "@in" else "@out" }
//                .map { (key, value) -> Pair(key, value.groupingBy { r -> r.type() }.eachCount()) }
//                .toMap(mutableMapOf())


//        log.info("count in and out:: {} ", inAndOutCountByEdgeType)

        val inAndOutCountByEdgeType: MutableMap<String, Map<String, Int>> = mutableMapOf()
        inAndOutCountByEdgeType.putIfAbsent("@in", mutableMapOf<String, Int>())
        inAndOutCountByEdgeType.putIfAbsent("@out", mutableMapOf<String, Int>())

        cyto.data.record.putAll(inAndOutCountByEdgeType)
        cyto.data.record["@edgeCount"] = 0
        return cyto
    }


    private fun toCytoData(node: Node): CytoData {

        val id = toArcadeId(dataSource, Neo4jType.NODE, node.id())

        val record = Sprite()
                .load(node.asMap())
                .rename("id", "_id_")

        val data = Data(id = id, record = record.asMap() as MutableMap<String, Any>)

        val cytoData = CytoData(data = data, classes = node.labels().joinToString(" "), group = "nodes")

        return cytoData
    }

    private fun toCytoData(rel: Relationship): CytoData {


        val id = toArcadeId(dataSource, Neo4jType.EDGE, rel.id())
        val source = toArcadeId(dataSource, Neo4jType.NODE, rel.startNodeId())
        val target = toArcadeId(dataSource, Neo4jType.NODE, rel.endNodeId())
        val record = Sprite()
                .load(rel.asMap())
                .rename("id", "_id_")

        val data = Data(id = id, record = record.asMap() as MutableMap<String, Any>, source = source, target = target)

        val cytoData = CytoData(data = data, classes = rel.type(), group = "edges")


        return cytoData
    }


    fun map(result: StatementResult): GraphData {

        log.info("mapping result max {} ", maxTraversal)
        val nodesClasses = mutableMapOf<String, Map<String, Any>>()
        val edgeClasses = mutableMapOf<String, Map<String, Any>>()
        val nodes = mutableSetOf<Node>()
        val rels = mutableSetOf<Relationship>()
        var fetchMore = true
        var fetched = 0
        while (result.hasNext() && fetchMore) {
            val record = result.next()

            record.values()
                    .asSequence()
                    .forEach { f ->
                        when (f.type().name()) {
                            "NODE" -> {
                                val node = f.asNode()

                                nodes.add(node)
                            }
                            "RELATIONSHIP" -> {
                                rels.add(f.asRelationship())
                            }
                            "PATH" -> {
                                val path = f.asPath()
                                rels.addAll(path.relationships())
                                nodes.addAll(path.nodes())

                            }
                        }
                    }

            if (fetched++ % 1000 == 0) {
                log.info("fethced:: {} ", fetched)
                log.info(" sizes {} . {} ", nodes.size, rels.size)
            }

            fetchMore = nodes.size < maxTraversal || rels.size < maxTraversal
        }


        log.info("nodes:: {} - rels:: {}", nodes.size, rels.size)
        val relsWithEachEnds = rels.asSequence()
                .filter { rel ->

                    val endNodeId = rel.endNodeId()

                    val startNodeId = rel.startNodeId()

                    var foundStart = false
                    var foundEnd = false

                    for (node in nodes) {
                        if (node.id() == endNodeId) {
                            foundEnd = true
                        } else if (node.id() == startNodeId) {
                            foundStart = true
                        }
                    }

                    foundStart && foundEnd
                }.toSet()

        val cytoNodes = nodes.asSequence()
                .map { c -> mapProperties(nodesClasses, c) }
                .map { n -> toCytoData(n) }
                .map { c -> countInOut(c, relsWithEachEnds) }
                .toSet()

        val edges = relsWithEachEnds.asSequence()
                .map { c -> mapProperties(edgeClasses, c) }
                .map { r -> toCytoData(r) }
                .toSet()

        val graphData = GraphData(nodesClasses, edgeClasses, cytoNodes, edges, !fetchMore)

        log.info("totals mapped: nodes {} - edges {} - truncated {} ", graphData.nodes.size, graphData.edges.size, graphData.truncated)

        return graphData
    }

    private fun mapProperties(nodesClasses: MutableMap<String, Map<String, Any>>, node: Node): Node {

        val className = node.labels().joinToString("_")
        (nodesClasses as java.util.Map<String, Map<String, Any>>).putIfAbsent(className, HashMap())
        val properties = nodesClasses[className]

        node.keys()
                .forEach { k -> (properties as java.util.Map<String, Any>).putIfAbsent(k, mapType(node.get(k).type().name())) }


        return node
    }

    private fun mapProperties(nodesClasses: MutableMap<String, Map<String, Any>>, rel: Relationship): Relationship {

        (nodesClasses as java.util.Map<String, Map<String, Any>>).putIfAbsent(rel.type(), HashMap())

        val properties = nodesClasses[rel.type()]

        rel.keys()
                .forEach { k -> (properties as java.util.Map<String, Any>).putIfAbsent(k, rel.get(k).type().name()) }

        return rel
    }

    companion object {

        private val log = LoggerFactory.getLogger(Neo4jDataProvider::class.java)
    }

}
