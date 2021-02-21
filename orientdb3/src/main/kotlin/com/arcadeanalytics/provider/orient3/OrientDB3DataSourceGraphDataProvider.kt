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
package com.arcadeanalytics.provider.orient3

import com.arcadeanalytics.provider.CytoData
import com.arcadeanalytics.provider.Data
import com.arcadeanalytics.provider.DataSourceGraphDataProvider
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.GraphData
import com.arcadeanalytics.provider.mapType
import com.google.common.collect.Maps
import com.orientechnologies.common.collection.OMultiValue
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.ODirection
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.record.impl.ODocumentHelper
import com.orientechnologies.orient.core.record.impl.OEdgeDocument
import com.orientechnologies.orient.core.record.impl.OVertexDocument
import com.orientechnologies.orient.core.sql.executor.OResultSet
import org.apache.commons.lang3.RegExUtils.removeFirst
import org.apache.commons.lang3.StringUtils.trimToEmpty
import org.apache.commons.lang3.StringUtils.wrap
import org.slf4j.LoggerFactory

/**
 * Specialized provider for OrientDB 3.0.x
 */
class OrientDB3DataSourceGraphDataProvider : DataSourceGraphDataProvider {

    private val log = LoggerFactory.getLogger(OrientDB3DataSourceGraphDataProvider::class.java)

    override fun supportedDataSourceTypes(): Set<String> = setOf("ORIENTDB3")

    override fun testConnection(dataSource: DataSourceInfo): Boolean {

        log.info("testing connection to :: '{}' ", dataSource.id)

        try {
            open(dataSource)
                .use { db ->

                    val count = db.query("SELECT FROM V LIMIT 1").asSequence().count()
                    log.info("connection works fine:: '{}' ", dataSource.id)
                    return count == 1
                }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    override fun fetchData(dataSource: DataSourceInfo, query: String, limit: Int): GraphData {

        log.info("fetching data from '{}' with query '{}' ", dataSource.id, query)

        open(dataSource)
            .use { db ->

                val lang = if (query.startsWith("gremlin:")) "gremlin" else "sql"

                db.execute(lang, query.removePrefix("gremlin:"))
                    .use { resultSet ->
                        val data = mapResultSet(dataSource, resultSet)
                        log.info("Fetched {} nodes and {} edges ", data.nodes.size, data.edges.size)
                        return data
                    }
            }
    }

    private fun toCytoData(element: OElement): CytoData {

        var record: MutableMap<String, Any> = when {
            element.isVertex -> transformToMap(element as OVertexDocument)
            else -> transformToMap(element as OEdgeDocument)
        }

        val ins = HashMap<String, Any>()
        record["@in"] = ins
        val outs = HashMap<String, Any>()
        record["@out"] = outs

        val keys = record.entries
            .asSequence()
            .filter { e -> e.key.startsWith("in_") }
            .map { e ->
                ins[removeFirst(e.key, "in_")] = e.value
                e.key
            }
            .toMutableSet()

        keys.addAll(
            record.entries
                .asSequence()
                .filter { e -> e.key.startsWith("out_") }
                .map { e ->
                    outs[removeFirst(e.key, "out_")] = e.value
                    e.key
                }.toSet()
        )

        keys.asSequence()
            .forEach { k -> record.remove(k) }

        cleanRecord(record)

        return when {
            element.isEdge() -> {
                val source = element.getProperty<String>("@outId")
                val target = element.getProperty<String>("@inId")
                val id = element.getProperty<String>("@id")
                val data = Data(id = id, record = record, source = source, target = target)
                CytoData(group = "edge", data = data, classes = element.getProperty("@class"))
            }
            else -> {
                val id = element.getProperty<String>("@id")
                val data = Data(id = id, record = record)
                CytoData(group = "nodes", data = data, classes = element.getProperty("@class"))
            }
        }
    }

    private fun cleanRecord(record: MutableMap<String, Any>) {

        record.remove("@type")
        record.remove("@rid")
        record.remove("@id")
        record.remove("@inId")
        record.remove("@outId")
        record.remove("@class")
        record.remove("@version")
        record.remove("@fieldtypes")
    }

    private fun transformToMap(doc: ODocument): MutableMap<String, Any> {
        val map = HashMap<String, Any>()
        for (property in doc.propertyNames) {
            val propertyType = doc.fieldType(property)
            if (propertyType == OType.LINK ||
                propertyType == OType.LINKBAG ||
                propertyType == OType.LINKLIST ||
                propertyType == OType.LINKSET ||
                propertyType == OType.LINKMAP
            ) continue

            var value = doc.field<Any>(property)

            if (value == null) continue

            if (value is ODocument)
                value = transformToMap(value)
            else if (value is ORID)
                value = value.toString()

            map[property] = value
        }

        val id = doc.identity
        if (id.isValid)
            map[ODocumentHelper.ATTRIBUTE_RID] = id.toString()

        val className = doc.schemaType
        if (className != null)
            map[ODocumentHelper.ATTRIBUTE_CLASS] = className

        return map
    }

    private fun mapField(doc: ODocument, fieldName: String): Any {

        val type = doc.fieldType(fieldName)

        if (type.isEmbedded) {
            doc.field<Any>(fieldName, OType.EMBEDDED)
        }

        return doc.field(fieldName)
    }

    fun mapResultSet(dataSource: DataSourceInfo, resultSet: OResultSet): GraphData {

        // DIVIDE VERTICES FROM EDGES
        val nodes = mutableSetOf<OVertex>()
        val edges = mutableSetOf<OEdge>()
        resultSet.asSequence()
            .onEach { res -> log.info(res.toJSON()) }
            .map { res -> res.element.get() }
            .forEach { element ->
                if (element.isVertex) {
                    val vertex: OVertex = element as OVertex
                    vertex.setProperty("@edgeCount", vertex.getEdges(ODirection.BOTH).asSequence().count())
                    nodes.add(vertex)
                } else if (element.isEdge) {
                    val edge: OEdge = element as OEdge
                    edges.add(edge)
//                        nodes.add(edge.from)
//                        nodes.add(edge.to)
                }
            }

        log.info("Computing edge map on {} edges...", edges.size)

        val edgeClasses = HashMap<String, Map<String, Any>>()
        val cytoEdges = edges.asSequence()
            .filter { e -> nodes.contains(e.from) && nodes.contains(e.to) }
            .map { e -> populateClasses(edgeClasses, e) }
            .map { e -> mapRid(dataSource, e) }
            .map { e -> mapInAndOut(dataSource, e) }
            .map { e -> countInAndOut(e) }
            .map { e -> toCytoData(e) }
            .toSet()

        log.info("Computing vertex map on {} vertices...", nodes.size)

        val nodeClasses = HashMap<String, Map<String, Any>>()
        val cytoNodes = nodes.asSequence()
            .map { v -> populateClasses(nodeClasses, v) }
            .map { v -> mapRid(dataSource, v) }
            .map { v -> countInAndOut(v) }
            .map { v -> toCytoData(v) }
            .toSet()

        return GraphData(nodeClasses, edgeClasses, cytoNodes, cytoEdges, false)
    }

    private fun countInAndOut(element: OElement): OElement {
        if (element.isVertex) {
            element as OVertexDocument
            element.fieldNames()
                .asSequence()
                .filter { f -> f.startsWith("out_") || f.startsWith("in_") }
                .forEach { f ->
                    val size = OMultiValue.getSize(element.field(f))
                    element.removeField(f)
                    element.field(f, size)
                }
        } else {
            element as OEdgeDocument
            element.fieldNames()
                .asSequence()
                .filter { f -> f.startsWith("out_") || f.startsWith("in_") }
                .forEach { f ->
                    val size = OMultiValue.getSize(element.field(f))
                    element.removeField(f)
                    element.field(f, size)
                }
        }

        return element
    }

    private fun mapInAndOut(dataSource: DataSourceInfo, element: OElement): OElement {
        element as OEdgeDocument

        if (!element.containsField("out"))
            return element

        val outRid = (element.rawField<Any>("out") as OIdentifiable).identity
        element.field("@outId", "${dataSource.id}_${outRid.clusterId}_${outRid.clusterPosition}")
        element.removeField("out")

        val inRid = (element.rawField<Any>("in") as OIdentifiable).identity
        element.field("@inId", "${dataSource.id}_${inRid.clusterId}_${inRid.clusterPosition}")
        element.removeField("in")

        return element
    }

    private fun mapRid(dataSource: DataSourceInfo, doc: OElement): OElement {
        val rid = doc.identity

        doc.setProperty("@id", "${dataSource.id}_${rid.clusterId}_${rid.clusterPosition}")

        return doc
    }

    private fun populateClasses(classes: MutableMap<String, Map<String, Any>>, element: OElement): OElement {

        classes.putIfAbsent(element.schemaType.get().toString(), Maps.newHashMap())

        populateProperties(classes, element as ODocument)

        return element
    }

    private fun populateProperties(classes: Map<String, Map<String, Any>>, vertex: ODocument) {

        val properties = classes[vertex.schemaType.get().toString()]

        vertex.propertyNames
            .asSequence()
            .filter { p -> !p.startsWith("@") }
            .filter { p -> !p.startsWith("in_") }
            .filter { p -> !p.startsWith("out_") }
            .filter { p ->
                val propertyType = vertex.fieldType(p)
                propertyType != OType.LINK &&
                    propertyType != OType.LINKMAP &&
                    propertyType != OType.LINKSET &&
                    propertyType != OType.LINKLIST &&
                    propertyType != OType.LINKBAG
            }
            .forEach { f ->
                val type = vertex.fieldType(f)
                if (type != null)
                    (properties as MutableMap<String, Any>).putIfAbsent(f, mapType(type.name))
            }
    }

    override fun expand(
        dataSource: DataSourceInfo,
        ids: Array<String>,
        direction: String,
        edgeLabel: String,
        maxTraversal: Int
    ): GraphData {

        val cleanedEdgeLabel = wrap(trimToEmpty(edgeLabel), "`")

        var query = "SELECT FROM (TRAVERSE "

        when (direction) {
            "out" -> query += "outE($cleanedEdgeLabel), inV()"
            "in" -> query += "inE($cleanedEdgeLabel), outV()"
            "both" -> query += "bothE($cleanedEdgeLabel), bothV()"
        }

        query += " FROM ["

        query += cleanIds(dataSource, ids)

        query += "] MAXDEPTH 2) LIMIT $maxTraversal"

        return fetchData(dataSource, query, maxTraversal)
    }

    override fun edges(
        dataSource: DataSourceInfo,
        fromIds: Array<String>,
        edgesLabel: Array<String>,
        toIds: Array<String>
    ): GraphData {
        val cleanedFromIds = cleanIds(dataSource, fromIds)
        val cleanedToIds = cleanIds(dataSource, toIds)
        val cleanLabels = edgesLabel.joinToString("','", "'", "'")

        val query =
            """MATCH {class: V, AS:node, WHERE: ( @rid IN [$cleanedFromIds] ) }
                    |.bothE($cleanLabels ) { AS: rel }
                    |.bothV() { AS: target, WHERE: ( @rid IN [$cleanedToIds] ) }
                    | RETURN ${'$'}pathElements
                    """.trimMargin()

        return fetchData(dataSource, query, 10000)
    }

    override fun load(dataSource: DataSourceInfo, ids: Array<String>): GraphData {

        var query = "SELECT FROM ["

        query += cleanIds(dataSource, ids)

        query += "] "

        return fetchData(dataSource, query, ids.size)
    }

    private fun cleanIds(dataSource: DataSourceInfo, ids: Array<String>): String {
        return ids
            .asSequence()
            .map { r -> r.removePrefix("${dataSource.id}_") }
            .map { r -> "#" + r.replace('_', ':') }
            .joinToString { it }
    }

    override fun loadFromClass(dataSource: DataSourceInfo, className: String, limit: Int): GraphData {
        val query = "select * from `$className` limit $limit"
        return fetchData(dataSource, query, limit)
    }

    override fun loadFromClass(dataSource: DataSourceInfo, className: String, propName: String, propertyValue: String, limit: Int): GraphData {
        val query = "select * from `$className` where `$propName` = '$propertyValue' limit $limit"
        return fetchData(dataSource, query, limit)
    }
}
