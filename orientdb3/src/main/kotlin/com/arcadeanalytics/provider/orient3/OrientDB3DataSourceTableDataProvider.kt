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
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.DataSourceTableDataProvider
import com.arcadeanalytics.provider.GraphData
import com.arcadeanalytics.provider.QueryParams
import com.arcadeanalytics.provider.TABLE_CLASS
import com.arcadeanalytics.provider.TypeProperty
import com.arcadeanalytics.provider.prefixIfAbsent
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocumentHelper
import com.orientechnologies.orient.core.sql.executor.OResult
import com.orientechnologies.orient.core.sql.executor.OResultSet
import org.apache.commons.lang3.StringUtils.truncate
import org.slf4j.LoggerFactory

/**
 * Specialized provider for OrientDB 3.0.x
 */
class OrientDB3DataSourceTableDataProvider : DataSourceTableDataProvider {
    private val log = LoggerFactory.getLogger(OrientDB3DataSourceTableDataProvider::class.java)

    override fun supportedDataSourceTypes(): Set<String> = setOf(ORIENTDB3)

    override fun fetchData(
        dataSource: DataSourceInfo,
        query: String,
        params: QueryParams,
        limit: Int,
    ): GraphData {
        var filledQuery = query
        params
            .asSequence()
            .forEach { p -> filledQuery = filledQuery.replace("${p.name.prefixIfAbsent(":")}", p.value) }

        return fetchData(dataSource, filledQuery, limit)
    }

    override fun fetchData(
        dataSource: DataSourceInfo,
        query: String,
        limit: Int,
    ): GraphData {
        log.info("fetching data from '{}' with query '{}' ", dataSource.id, truncate(query, 256))

        open(dataSource)
            .use { db ->

                val lang = if (query.startsWith("gremlin:")) "gremlin" else "sql"

                db
                    .execute(lang, query.removePrefix("gremlin:"))
                    .use { resultSet ->

                        val data = mapResultSet(resultSet)

                        log.info("Fetched {} rows", data.nodes.size)

                        return data
                    }
            }
    }

    fun mapResultSet(resultSet: OResultSet): GraphData {
        val nodesProperties = mutableMapOf<String, TypeProperty>()

        var count: Long = 0
        val cytoNodes =
            resultSet
                .asSequence()
                .map { v -> populateProperties(nodesProperties, v) }
                .map { v -> toCytoData(v, count++) }
                .toSet()

        log.debug("properties:: {} ", nodesProperties)
        val tableClass = mutableMapOf<String, Any>()
        tableClass.put("name", TABLE_CLASS)
        tableClass.put("cardinality", count)
        tableClass.put("properties", nodesProperties)

        val nodeClasses = mutableMapOf<String, MutableMap<String, Any>>()
        nodeClasses.put(TABLE_CLASS, tableClass)

        val edgeClasses = mutableMapOf<String, MutableMap<String, Any>>()
        val cytoEdges = emptySet<CytoData>()

        return GraphData(nodeClasses, edgeClasses, cytoNodes, cytoEdges)
    }

    private fun populateProperties(
        properties: MutableMap<String, TypeProperty>,
        element: OResult,
    ): OResult {
        val props =
            element.propertyNames
                .asSequence()
                .filter { name -> !properties.containsKey(name) }
                .filter { p -> !p.startsWith("@") }
                .filter { p -> !p.startsWith("in_") }
                .filter { p -> !p.startsWith("out_") }
                .map { name ->

                    val property = element.getProperty<Any>(name)

                    val type = property.javaClass.simpleName

                    name to TypeProperty(name, type)
                }.toMap()

        properties.putAll(props)

        return element
    }

    private fun toCytoData(
        element: OResult,
        index: Long,
    ): CytoData {
        val record: MutableMap<String, Any> = transformToMap(element)

        cleanRecord(record)

        val data = Data(id = index.toString(), record = record)
        return CytoData(group = "nodes", data = data, classes = TABLE_CLASS)
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

    private fun transformToMap(doc: OResult): MutableMap<String, Any> {
        val record = HashMap<String, Any>()
        doc.propertyNames
            .asSequence()
            .filter { p -> !p.startsWith("@") }
            .filter { p -> !p.startsWith("in_") }
            .filter { p -> !p.startsWith("out_") }
            .filter { property ->
                val propertyType = doc.getMetadata(property)
                return@filter propertyType != OType.LINK ||
                    propertyType != OType.LINKBAG ||
                    propertyType != OType.LINKLIST ||
                    propertyType != OType.LINKSET ||
                    propertyType != OType.LINKMAP
            }.filter { property -> doc.getProperty<Any>(property) != null }
            .forEach { property ->

                var value = doc.getProperty<Any>(property)

                record[property] = value
            }

        record[ODocumentHelper.ATTRIBUTE_RID] =
            doc.identity
                .filter { id -> id.isValid }
                .map { id -> id.toString() }

        record[ODocumentHelper.ATTRIBUTE_CLASS] = TABLE_CLASS

        return record
    }
}
