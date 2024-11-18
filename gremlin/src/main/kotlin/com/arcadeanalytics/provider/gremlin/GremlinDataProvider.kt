/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2021 ArcadeData
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
package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.provider.CytoData
import com.arcadeanalytics.provider.Data
import com.arcadeanalytics.provider.DataSourceGraphDataProvider
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.GraphData
import com.arcadeanalytics.provider.mapType
import com.arcadeanalytics.provider.prefixIfAbsent
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.StringUtils.wrap
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.Result
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.slf4j.LoggerFactory
import java.util.HashMap
import java.util.HashSet

class GremlinDataProvider : DataSourceGraphDataProvider {
    private val log = LoggerFactory.getLogger(GremlinDataProvider::class.java)

    override fun fetchData(
        dataSource: DataSourceInfo,
        query: String,
        limit: Int,
    ): GraphData {
        val cluster = getCluster(dataSource)

        val client = cluster.connect<Client>().init()
        try {
            return getGraphData(dataSource, query, limit, client)
        } catch (e: Exception) {
            throw RuntimeException(e)
        } finally {
            client.close()
            cluster.close()
        }
    }

    private fun getGraphData(
        dataSource: DataSourceInfo,
        query: String,
        limit: Int,
        client: Client,
    ): GraphData {
        val cytoNodes = HashSet<CytoData>()
        val cytoEdges = HashSet<CytoData>()
        val edgeClasses = HashMap<String, Map<String, Any>>()
        val nodeClasses = HashMap<String, Map<String, Any>>()

        log.info("fetching data from '{}' with query '{}' ", dataSource.id, query)

        val ids: Array<String> =
            client
                .submit(query)
                .asSequence()
                .take(limit)
                .map { r -> toCytoData(dataSource, r, client) }
                .map { data ->
                    if (data.group == "nodes") {
                        cytoNodes.add(data)
                        populateClasses(nodeClasses, data)
                        emptyList<String>()
                    } else {
                        cytoEdges.add(data)

                        populateClasses(edgeClasses, data)
                        listOf(data.data.source, data.data.target)
                    }
                }.flatMap { it.asSequence() }
                .toSet()
                .toTypedArray()

        if (ids.isNotEmpty()) {
            val load = load(dataSource, ids, client)

            load.nodes
                .asSequence()
                .forEach { n ->
                    cytoNodes.add(n)
                    populateClasses(nodeClasses, n)
                }
        }

        val graphData = GraphData(nodeClasses, edgeClasses, cytoNodes, cytoEdges)

        log.info("fetched {} nodes and {} edges ", cytoNodes.size, cytoEdges.size)
        return graphData
    }

    private fun toCytoData(
        dataSource: DataSourceInfo,
        result: Result,
        client: Client,
    ): CytoData {
        val element = result.element

        // id clean
        val id = nativeIdToArcadeId(dataSource, element.id().toString())

        val record = transformToMap(element)

        val ins = HashMap<String, Any>()
        record["@in"] = ins
        val outs = HashMap<String, Any>()
        record["@out"] = outs

        if (record.containsKey("id")) {
            record["_id_"] = record["id"]!!
            record.remove("id")
        }

        val cyto =
            when (element) {
                is Vertex -> {
                    outs.putAll(
                        client
                            .submit("g.V('${element.id()}').outE()")
                            .asSequence()
                            .map { r1 -> r1.edge }
                            .map { e1 -> e1.label() }
                            .groupingBy { it }
                            .eachCount(),
                    )

                    ins.putAll(
                        client
                            .submit("g.V('${element.id()}').inE()")
                            .asSequence()
                            .map { r1 -> r1.edge }
                            .map { e1 -> e1.label() }
                            .groupingBy { it }
                            .eachCount(),
                    )

                    var edgeCount =
                        ins.values
                            .asSequence()
                            .map { o -> o as Int }
                            .sum()
                    edgeCount +=
                        outs.values
                            .asSequence()
                            .map { o -> o as Int }
                            .sum()
                    record["@edgeCount"] = edgeCount

                    val data = Data(id = id, record = record)
                    CytoData(classes = element.label(), data = data, group = "nodes")
                }
                is Edge -> {
                    val sourceId = element.outVertex().id().toString()
                    val source = nativeIdToArcadeId(dataSource, sourceId)

                    val targetId = element.inVertex().id().toString()
                    val target = nativeIdToArcadeId(dataSource, targetId)

                    val data = Data(id = id, source = source, target = target, record = record)
                    CytoData(classes = element.label(), data = data, group = "edges")
                }
                else -> {
                    log.info("element not mappable:: $element")
                    CytoData(classes = element.label(), data = Data(id), group = "unk")
                }
            }
        return cyto
    }

    private fun populateClasses(
        classes: MutableMap<String, Map<String, Any>>,
        element: CytoData,
    ): CytoData {
        if (classes[element.classes] == null) {
            classes[element.classes] = Maps.newHashMap()
        }
        populateProperties(classes, element)
        return element
    }

    private fun populateProperties(
        classes: Map<String, Map<String, Any>>,
        element: CytoData,
    ) {
        val properties = classes[element.classes]

        element.data.record.keys
            .asSequence()
            .filter { f -> !f.startsWith("@") }
            .filter { f -> !f.startsWith("in_") }
            .filter { f -> !f.startsWith("out_") }
            .forEach { f -> (properties as java.util.Map<String, Any>).putIfAbsent(f, mapType("STRING")) }
    }

    private fun transformToMap(doc: Element): MutableMap<String, Any> {
        val record = Maps.newHashMap<String, Any>()
        doc
            .keys()
            .asSequence()
            .forEach { k ->
                // take single value
                record[k] = doc.properties<Any>(k).next().value()
            }

        return record
    }

    override fun load(
        dataSource: DataSourceInfo,
        ids: Array<String>,
    ): GraphData {
        val query = loadQuery(dataSource, ids)

        return fetchData(dataSource, query, ids.size)
    }

    private fun load(
        dataSource: DataSourceInfo,
        ids: Array<String>,
        client: Client,
    ): GraphData {
        val query = loadQuery(dataSource, ids)

        return getGraphData(dataSource, query, ids.size, client)
    }

    override fun loadFromClass(
        dataSource: DataSourceInfo,
        className: String,
        limit: Int,
    ): GraphData {
        val query = "g.V().hasLabel(${splitMultilabel(className)}).limit($limit)"

        return fetchData(dataSource, query, limit)
    }

    override fun loadFromClass(
        dataSource: DataSourceInfo,
        className: String,
        propName: String,
        propertyValue: String,
        limit: Int,
    ): GraphData {
        val query = "g.V().hasLabel(${splitMultilabel(className)}).has('$propName', eq('$propertyValue')).limit($limit)"

        return fetchData(dataSource, query, limit)
    }

    private fun loadQuery(
        dataSource: DataSourceInfo,
        ids: Array<String>,
    ): String {
        check(ids.isNotEmpty())
        log.debug("load ids {} ", *ids)

        val joinedIds = cleanIds(dataSource, ids)

        return "g.V($joinedIds)"
    }

    private fun cleanIds(
        dataSource: DataSourceInfo,
        ids: Array<String>,
    ): String =
        ids
            .asSequence()
            .map { id -> arcadeIdToNativeId(dataSource, id) }
            .map { id -> """ '$id' """ }
            .joinToString(",")

    private fun nativeIdToArcadeId(
        dataSource: DataSourceInfo,
        id: String,
    ): String =
        when (dataSource.type) {
            "GREMLIN_ORIENTDB" -> "${dataSource.id}_${id.removePrefix("#").replace(":", "_")}"
            else -> "${dataSource.id}_$id"
        }

    private fun arcadeIdToNativeId(
        dataSource: DataSourceInfo,
        id: String,
    ): String =
        when (dataSource.type) {
            "GREMLIN_ORIENTDB" -> id.removePrefix("${dataSource.id}_").prefixIfAbsent("#").replace("_", ":")
            else -> id.removePrefix("${dataSource.id}_")
        }

    override fun expand(
        dataSource: DataSourceInfo,
        roots: Array<String>,
        direction: String,
        edgeLabel: String,
        maxTraversal: Int,
    ): GraphData {
        var edgeLabel = edgeLabel

        var query = loadQuery(dataSource, roots)

        if (StringUtils.isNotEmpty(edgeLabel)) edgeLabel = wrap(edgeLabel, '"')

        query +=
            when (direction.toLowerCase()) {
                "out" -> """.outE($edgeLabel)"""
                "in" -> """.inE($edgeLabel)"""
                else -> """.bothE($edgeLabel)"""
            }

        return fetchData(dataSource, query, maxTraversal)
    }

    override fun edges(
        dataSource: DataSourceInfo,
        fromIds: Array<String>,
        edgesLabel: Array<String>,
        toIds: Array<String>,
    ): GraphData {
        val cleanLabels = edgesLabel.joinToString("','", "'", "'")

        val query =
            """
            g.V(${cleanIds(dataSource, fromIds)})
                .bothE()
                .where(inV().has(id, within(${cleanIds(dataSource, toIds)} )))
                .hasLabel($cleanLabels)
            """.trimIndent()

        return fetchData(dataSource, query, 10000)
    }

    override fun testConnection(dataSource: DataSourceInfo): Boolean {
        try {
            log.info("testing connection to :: '{}' ", dataSource.server)

            val cluster = getCluster(dataSource)

            cluster.availableHosts().stream().forEach { h -> println("h = $h") }

            val client = cluster.connect<Client>().init()

            client.submit("g.V().count()")

            client.close()

            cluster.close()

            log.info("connection works fine:: '{}' ", dataSource.server)

            return true
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    override fun supportedDataSourceTypes(): Set<String> = Sets.newHashSet("GREMLIN_ORIENTDB", "GREMLIN_NEPTUNE", "GREMLIN_JANUSGRAPH")
}
