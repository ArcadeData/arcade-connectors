/*-
 * #%L
 * Arcade Connectors
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
package com.arcadeanalytics.provider.orient2

import com.arcadeanalytics.provider.*
import com.google.common.collect.Maps
import com.orientechnologies.common.collection.OMultiValue
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.record.impl.ODocumentHelper
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery
import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientEdge
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import org.apache.commons.lang3.RegExUtils.removeFirst
import org.apache.commons.lang3.StringUtils.*
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Specialized provider for OrientDB2
 * @author Roberto Franchini
 */
class OrientDBDataSourceGraphDataProvider : DataSourceGraphDataProvider {

    private val log = LoggerFactory.getLogger(OrientDBDataSourceGraphDataProvider::class.java)

    override fun supportedDataSourceTypes(): Set<String> = setOf("ORIENTDB")

    override fun testConnection(dataSource: DataSourceInfo): Boolean {

        log.info("testing connection to data source '{}' ", dataSource.id)

        try {

            fetchData(dataSource, "SELECT FROM V LIMIT 1", 1)
            log.info("connection to data source '{}' works fine", dataSource.id)

        } catch (e: Exception) {
            throw RuntimeException(e)
        }

        return true
    }


    override fun fetchData(dataSource: DataSourceInfo, query: String, limit: Int): GraphData {

        open(dataSource)
                .use { db ->
                    log.info("fetching data from '{}' with query '{}' ", dataSource.id, truncate(query, 256))
                    val collector = OrientDBDocumentCollector()

                    db.query<List<*>>(OSQLAsynchQuery<Any>(query, OrientDBResultListener(collector, limit)))

                    log.info("Query executed, returned {} records with limit {} ", collector.size(), limit)

                    val data = mapResultSet(dataSource, db, collector)
                    log.info("Fetched {} nodes and {} edges ", data.nodes.size, data.edges.size)

                    return data

                }

    }

    private fun toData(doc: ODocument): CytoData {

        val record = transformToMap(doc)

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

        keys.addAll(record.entries
                .asSequence()
                .filter { e -> e.key.startsWith("out_") }
                .map { e ->
                    outs[removeFirst(e.key, "out_")] = e.value
                    e.key
                }.toSet())

        keys.asSequence()
                .forEach { k -> record.remove(k) }

        cleanRecord(record)

        return when {
            doc.isEdgeType() -> {
                val source = doc.field<String>("@outId")
                val target = doc.field<String>("@inId")
                val id = doc.field<String>("@id")
                val data = Data(id = id, record = record, source = source, target = target)
                CytoData(group = "edge", data = data, classes = doc.field("@class"))
            }
            else -> {
                val id = doc.field<String>("@id")
                val data = Data(id = id, record = record)
                CytoData(group = "nodes", data = data, classes = doc.field("@class"))
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
        for (field in doc.fieldNames()) {
            val fieldType = doc.fieldType(field)
            if (fieldType == OType.LINK ||
                    fieldType == OType.LINKBAG ||
                    fieldType == OType.LINKLIST ||
                    fieldType == OType.LINKSET ||
                    fieldType == OType.LINKMAP
            ) continue

            var value = doc.field<Any>(field)

            if (value == null) continue

            if (value is ODocument)
                value = transformToMap(value)
            else if (value is ORID)
                value = value.toString()

            map[field] = value
        }

        val id = doc.identity
        if (id.isValid)
            map[ODocumentHelper.ATTRIBUTE_RID] = id.toString()

        val className = doc.className
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

    private fun mapResultSet(dataSource: DataSourceInfo,
                             db: ODatabaseDocumentTx,
                             collector: OrientDBDocumentCollector): GraphData {

        val graph = OrientGraphNoTx(db)

        // DIVIDE VERTICES FROM EDGES
        val nodes = mutableSetOf<OrientVertex>()
        val edges = mutableSetOf<OrientEdge>()
        val resultSet = collector.collected()

        resultSet.asSequence()
                .forEach { doc ->
                    if (doc.isVertexType()) {
                        val vertex = graph.getVertex(doc)
                        vertex.record.isTrackingChanges = false
                        vertex.record.field("@edgeCount", vertex.countEdges(Direction.BOTH).toInt())
                        nodes.add(vertex)
                    } else if (doc.isEdgeType()) {
                        val edge = graph.getEdge(doc)
                        edges.add(edge)
                        nodes.add(graph.getVertex(edge.getVertex(Direction.IN)))
                        nodes.add(graph.getVertex(edge.getVertex(Direction.OUT)))
                    }
                }

        log.info("Computing edge map on {} edges...", edges.size)

        val edgeClasses = mutableMapOf<String, Map<String, Any>>()
        val cytoEdges = edges.asSequence()
                .map { e -> e.record }
                .map { d -> populateClasses(edgeClasses, d) }
                .map { d -> mapRid(dataSource, d) }
                .map { d -> mapInAndOut(dataSource, d) }
                .map { d -> countInAndOut(d) }
                .map { d -> toData(d) }
                .toSet()

        log.info("Computing vertex map on {} vertices...", nodes.size)

        val nodeClasses = mutableMapOf<String, Map<String, Any>>()
        val cytoNodes = nodes.asSequence()
                .map { e -> e.record }
                .map { d -> populateClasses(nodeClasses, d) }
                .map { d -> mapRid(dataSource, d) }
                .map { d -> countInAndOut(d) }
                .map { d -> toData(d) }
                .toSet()


        return GraphData(nodeClasses, edgeClasses, cytoNodes, cytoEdges, collector.isTruncated)
    }

    private fun addConnectedVertex(connectedVertices: MutableSet<ODocument>, vertex: OrientVertex) {
        val record = vertex.record
        if (connectedVertices.add(record)) {
            record.isTrackingChanges = false
            record.field("@edgeCount", vertex.countEdges(Direction.BOTH))
        }
    }

    private fun countInAndOut(doc: ODocument): ODocument {

        doc.fieldNames()
                .asSequence()
                .filter { f -> f.startsWith("out_") || f.startsWith("in_") }
                .forEach { f ->
                    val size = OMultiValue.getSize(doc.field(f))
                    doc.removeField(f)
                    doc.field(f, size)
                }

        return doc
    }

    private fun clean(d: ODocument): ODocument {

        for (f in d.fieldNames()) {
            val fieldValue = d.field<Any>(f)
            if (fieldValue is ORidBag || fieldValue is OIdentifiable)
            // IGNORE LINKS
                d.removeField(f)
        }
        d.detach()
        return d
    }

    private fun mapInAndOut(dataSource: DataSourceInfo, d: ODocument): ODocument {
        if (!d.containsField("out"))
            return d

        val outRid = (d.rawField<Any>("out") as OIdentifiable).identity
        d.field("@outId", "${dataSource.id}_${outRid.clusterId}_${outRid.clusterPosition}")
        d.removeField("out")

        val inRid = (d.rawField<Any>("in") as OIdentifiable).identity
        d.field("@inId", "${dataSource.id}_${inRid.clusterId}_${inRid.clusterPosition}")
        d.removeField("in")
        return d
    }

    private fun mapRid(dataSource: DataSourceInfo, doc: ODocument): ODocument {
        val rid = doc.identity

        doc.field("@id", "${dataSource.id}_${rid.clusterId}_${rid.clusterPosition}")

        return doc
    }

    private fun populateClasses(classes: MutableMap<String, Map<String, Any>>, element: ODocument): ODocument {

        classes.putIfAbsent(element.className, Maps.newHashMap())

        populateProperties(classes, element)
        return element
    }

    private fun populateProperties(classes: Map<String, Map<String, Any>>, element: ODocument) {

        val properties = classes[element.className] as MutableMap

        element.fieldNames()
                .asSequence()
                .filter { f -> !f.startsWith("@") }
                .filter { f -> !f.startsWith("in_") }
                .filter { f -> !f.startsWith("out_") }
                .filter { f ->
                    val fieldType = element.fieldType(f)
                    fieldType != OType.LINK &&
                            fieldType != OType.LINKMAP &&
                            fieldType != OType.LINKSET &&
                            fieldType != OType.LINKLIST &&
                            fieldType != OType.LINKBAG
                }
                .forEach { f ->
                    val type = element.fieldType(f)
                    if (type != null)
                        properties.putIfAbsent(f, mapType(type.name))
                }

    }


    override fun expand(dataSource: DataSourceInfo,
                        ids: Array<String>,
                        direction: String,
                        edgeLabel: String,
                        maxTraversal: Int): GraphData {

        val cleanedEdgeLabel = wrap(trimToEmpty(edgeLabel), "'")

        var query = "SELECT FROM (TRAVERSE "

        when (direction) {
            "out" -> query += "outE($cleanedEdgeLabel), inV()"
            "in" -> query += "inE($cleanedEdgeLabel), outV()"
            "both" -> query += "bothE($cleanedEdgeLabel), bothV()"
        }

        query += " FROM ["

        query += ids
                .asSequence()
                .map { r -> r.removePrefix("${dataSource.id}_") }
                .map { r -> "#" + r.replace('_', ':') }
                .joinToString { it }

        query += "] MAXDEPTH 2) LIMIT $maxTraversal"

        return fetchData(dataSource, query, maxTraversal)

    }

    override fun load(dataSource: DataSourceInfo, ids: Array<String>): GraphData {

        var query = "SELECT FROM ["

        query += ids
                .asSequence()
                .map { r -> r.removePrefix("${dataSource.id}_") }
                .map { r -> "#" + r.replace('_', ':') }
                .joinToString { it }

        query += "] "

        return fetchData(dataSource, query, ids.size)

    }


    override fun loadFromClass(dataSource: DataSourceInfo, className: String, limit: Int): GraphData {
        val query = "select * from $className limit $limit"
        return fetchData(dataSource, query, limit)
    }

    override fun loadFromClass(dataSource: DataSourceInfo, className: String, propName: String, propValue: String, limit: Int): GraphData {
        val query = "select * from $className where $propName = '$propValue' limit $limit"
        return fetchData(dataSource, query, limit)
    }


}
