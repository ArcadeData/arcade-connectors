package com.arcadeanalytics.provider.gremlin.cosmosdb
/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2020 ArcadeData
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

import com.arcadeanalytics.provider.*
import com.arcadeanalytics.provider.gremlin.createSerializer
import com.google.common.collect.Maps
import com.google.common.collect.Sets
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.StringUtils.removeStart
import org.apache.commons.lang3.StringUtils.wrap
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.Result
import org.slf4j.LoggerFactory
import java.util.*

class CosmosDBGremlinDataProvider : DataSourceGraphDataProvider {

    private val log = LoggerFactory.getLogger(CosmosDBGremlinDataProvider::class.java)

    override fun supportedDataSourceTypes(): Set<String> {
        return Sets.newHashSet("GREMLIN_COSMOSDB")
    }


    override fun fetchData(dataSource: DataSourceInfo, query: String, limit: Int): GraphData {

        val cluster = getCluster(dataSource)

        log.info("fetching data from '{}' with query '{}' ", dataSource.id, query)

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

    private fun getCluster(dataSource: DataSourceInfo): Cluster {


        val serializer = createSerializer(dataSource)


        return Cluster.build(dataSource.server)
                .port(dataSource.port)
                .serializer(serializer)
                .enableSsl(dataSource.type == "GREMLIN_COSMOSDB")
                .credentials(dataSource.username, dataSource.password)
                .maxWaitForConnection(10000)
                .create()
    }


    private fun getGraphData(dataSource: DataSourceInfo, query: String, limit: Int, client: Client): GraphData {
        val nodes = HashSet<CytoData>()
        val edges = HashSet<CytoData>()
        val edgeClasses = HashMap<String, Map<String, Any>>()
        val nodeClasses = HashMap<String, Map<String, Any>>()

        //        final Contract contract = dataSource.getWorkspace().getUser().getCompany().getContract();

        client.submit(query)
                .stream()
                .limit(limit.toLong())
                .map { r -> toData(dataSource, r, client) }
                .forEach { data ->
                    if (data.group == "nodes") {
                        nodes.add(data)
                        populateClasses(nodeClasses, data)
                    } else {
                        edges.add(data)

                        val load = load(dataSource, arrayOf(data.data.source, data.data.target), client)

                        load.nodes
                                .stream()
                                .forEach { n ->
                                    nodes.add(n)
                                    populateClasses(nodeClasses, n)
                                }

                        populateClasses(edgeClasses, data)
                    }
                }

        val graphData = GraphData(nodeClasses, edgeClasses, nodes, edges)

        log.info("fetched {} nodes and {} edges ", nodes.size, edges.size)
        return graphData
    }


    private fun toData(dataSource: DataSourceInfo, result: Result, client: Client): CytoData {

        log.info("result:: {}", result)

        val record = transformToMap(result)

        log.info("record:: {}", record)
        //id clean
        val id = cleanOrientId(record["id"].toString())

        val ins = HashMap<String, Any>()
        record["@in"] = ins
        val outs = HashMap<String, Any>()
        record["@out"] = outs


        log.info("element map:: $result")
        if (record.containsKey("id")) {
            record["_id_"] = record["id"]!!
            record.remove("id")
        }


        val cyto = when (record["type"]) {

            "vertex" -> {
                outs.putAll(client.submit("g.V('${record["id"]}').outE()").asSequence()
                        .map { r1 -> r1.getObject() as Map<String, Any> }
                        .map { e1 -> e1["label"].toString() }
                        .groupingBy { it }
                        .eachCount())

                ins.putAll(client.submit("g.V('${record["id"]}').inE()").asSequence()
                        .map { r1 -> r1.getObject() as Map<String, Any> }
                        .map { e1 -> e1["label"].toString() }
                        .groupingBy { it }
                        .eachCount())

                var edgeCount = ins.values.stream().mapToLong { o -> o as Long }.sum()
                edgeCount += outs.values.stream().mapToLong { o -> o as Long }.sum()
                record["@edgeCount"] = edgeCount

                val data = Data(id = id, record = record)
                CytoData(classes = record["label"].toString(), data = data, group = "nodes")

            }

            "edge" -> {

                val sourceId = record["inV"].toString()
                val source = dataSource.id.toString() + "_" + cleanOrientId(sourceId)
                val targetId = record["outV"].toString()
                val target = dataSource.id.toString() + "_" + cleanOrientId(targetId)

                record.remove("outV")
                record.remove("inV")

                val data = Data(id = id, source = source, target = target, record = record)
                CytoData(classes = record["label"].toString(), data = data, group = "edges")

            }
            else -> {
                log.info("element not mappable:: $record")
                CytoData(classes = record["label"].toString(), data = Data(id), group = "unk")
            }
        }
        return cyto
    }

    private fun cleanOrientId(id: String): String {
        return removeStart(id, "#")
                .replace(":", "_")
    }

    protected fun populateClasses(classes: MutableMap<String, Map<String, Any>>, element: CytoData): CytoData {
        if (classes[element.classes] == null) {
            classes[element.classes] = Maps.newHashMap()
        }
        //        populatePropertyValues(classes, element, stats);
        return element
    }


    private fun transformToMap(result: Result): MutableMap<String, Any> {

        val res = result.getObject() as MutableMap<String, Any>

        if (res.containsKey("properties")) {

            if (res["type"].toString() == "vertex") {
                val props = res["properties"] as Map<String, List<Map<String, Any>>>
                props.forEach { k, v -> if (v is List<*>) res.put(k, v[0]["value"]!!) }
            } else {
                val props = res["properties"] as Map<String, Any>
                props.forEach { k, v -> res.put(k, v) }


            }
            res.remove("properties")
            res.remove("class")
        }
        return res


    }

    override fun load(dataSource: DataSourceInfo, ids: Array<String>): GraphData {
        val query = loadQuery(dataSource, ids)

        val cluster = getCluster(dataSource)
        val client = cluster.connect<Client>().init()

        log.info("fetching data from '{}' with query '{}' ", cluster.availableHosts()[0], query)

        try {

            return load(dataSource, ids, client)
        } catch (e: Exception) {
            throw RuntimeException(e)
        } finally {
            client.close()
            cluster.close()
        }

    }

    private fun load(dataSource: DataSourceInfo, ids: Array<String>, client: Client): GraphData {
        val query = loadQuery(dataSource, ids)



        return getGraphData(dataSource, query, ids.size, client)

    }

    override fun loadFromClass(dataSource: DataSourceInfo, className: String, limit: Int): GraphData {
        val query = "g.V().hasLabel('$className').limit($limit)"
        return this.fetchData(dataSource, query, limit)
    }

    override fun loadFromClass(dataSource: DataSourceInfo, className: String, propName: String, propertyValue: String, limit: Int): GraphData {
        val query = "g.V().hasLabel('$className').has('$propName', eq('$propertyValue')).limit($limit)"
        return this.fetchData(dataSource, query, limit)
    }

    private fun loadQuery(dataSource: DataSourceInfo, ids: Array<String>): String {
        var query = "g.V("

        log.debug("load ids {} ", *ids)

        query += ids.asSequence()
                .map { id -> removeStart(id, dataSource.id.toString() + "_") }
                .map { id -> arcadeIdToNativeId(dataSource, id) }
                .map { id -> """"$id"""" }
                .joinToString(",")

        query += ") "
        return query
    }

    private fun arcadeIdToNativeId(dataSource: DataSourceInfo, id: String): String {
        return if (dataSource.type == "GREMLIN_ORIENTDB") StringUtils.prependIfMissing(id, "#").replace("_", ":") else id
    }

    override fun expand(dataSource: DataSourceInfo, roots: Array<String>,
                        direction: String,
                        edgeLabel: String,
                        maxTraversal: Int): GraphData {
        var edgeLabel = edgeLabel
        var query = "g.V("

        query += roots.asSequence()
                .map { id -> removeStart(id, dataSource.id.toString() + "_") }
                .map { id -> arcadeIdToNativeId(dataSource, id) }
                .map { id -> wrap(id, '"') }
                .joinToString(",")
        query += ")"

        if (StringUtils.isNotEmpty(edgeLabel)) edgeLabel = wrap(edgeLabel, '"')

        query += when (direction.toLowerCase()) {
            "out" -> """.outE($edgeLabel) """
            "in" -> """.inE($edgeLabel) """
            else -> """.bothE($edgeLabel) """
        }


        return fetchData(dataSource, query, maxTraversal)

    }

    override fun edges(dataSource: DataSourceInfo, fromIds: Array<String>, edgesLabel: Array<String>, toIds: Array<String>): GraphData {
        TODO("Not yet implemented")
    }


    override fun testConnection(dataSource: DataSourceInfo): Boolean {
        try {
            log.info("testing connection to :: '{}' ", dataSource.server)
            val cluster = Cluster.build(dataSource.server)
                    .port(dataSource.port)
                    .enableSsl(true)
                    .serializer(createSerializer(dataSource))
                    .credentials(dataSource.username, dataSource.password)
                    .maxWaitForConnection(10000)
                    .create()

            cluster.availableHosts()
                    .stream().forEach { h -> log.info("gremlin host:: {} ", h) }

            val client = cluster.connect<Client>().init()

            client.close()

            cluster.close()

            log.info("connection works fine:: '{}' ", dataSource.server)

            return true
        } catch (e: Exception) {
            throw RuntimeException(e)
        }


    }
}
