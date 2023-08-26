package com.arcadeanalytics.provider.rdbms

import com.arcadeanalytics.provider.CytoData
import com.arcadeanalytics.provider.Data
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.DataSourceTableDataProvider
import com.arcadeanalytics.provider.GraphData
import com.arcadeanalytics.provider.QueryParams
import com.arcadeanalytics.provider.TABLE_CLASS
import com.arcadeanalytics.provider.TypeProperty
import com.arcadeanalytics.provider.mapType
import com.arcadeanalytics.provider.prefixIfAbsent
import com.arcadeanalytics.provider.rdbms.persistence.util.DBSourceConnection
import java.sql.ResultSet
import java.sql.ResultSetMetaData

class RDBMSTableDataProvider : DataSourceTableDataProvider {

    override fun fetchData(dataSource: DataSourceInfo, query: String, limit: Int): GraphData {
        DBSourceConnection.getConnection(dataSource).use { conn ->

            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->

                stmt.executeQuery(query).use { rs ->

                    val md = rs.metaData
                    val columns = md.columnCount

                    val cytoNodes = mapRows(rs, columns, md)

                    val nodesProperties = mapMetadata(columns, md)

                    val tableClass = mutableMapOf<String, Any>()
                    tableClass["name"] = TABLE_CLASS
                    tableClass["cardinality"] = cytoNodes.size
                    tableClass["properties"] = nodesProperties
                    val nodeClasses = mutableMapOf<String, MutableMap<String, Any>>()
                    nodeClasses[TABLE_CLASS] = tableClass

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

                    val edgeClasses = mutableMapOf<String, MutableMap<String, Any>>()
                    val cytoEdges = emptySet<CytoData>()

                    return GraphData(nodeClasses, edgeClasses, cytoNodes, cytoEdges)
                }
            }
        }
    }

    private fun mapMetadata(columns: Int, md: ResultSetMetaData): MutableMap<String, TypeProperty> {
        val nodesProperties = mutableMapOf<String, TypeProperty>()

        for (i in 1..columns) {
            val columnName = md.getColumnName(i)
            val columnTypeName = md.getColumnTypeName(i)

            nodesProperties[columnName] = TypeProperty(name = columnName, type = mapType(columnTypeName))
        }
        return nodesProperties
    }

    private fun mapRows(rs: ResultSet, columns: Int, md: ResultSetMetaData): MutableSet<CytoData> {
        val cytoNodes = mutableSetOf<CytoData>()
        var cardinality: Long = 0
        while (rs.next()) {
            val record = mutableMapOf<String, Any>()
            for (i in 1..columns) {
                record[md.getColumnName(i)] = rs.getObject(i)
            }

            val data = Data(id = cardinality++.toString(), record = record)

            val cytoData = CytoData(
                classes = TABLE_CLASS,
                group = "nodes",
                data = data,
            )

            cytoNodes.add(cytoData)
        }
        return cytoNodes
    }

    override fun fetchData(dataSource: DataSourceInfo, query: String, params: QueryParams, limit: Int): GraphData {
        var filledQuery = query
        params.asSequence()
            .forEach { p -> filledQuery = filledQuery.replace("${p.name.prefixIfAbsent(":")}", p.value) }

        return fetchData(dataSource, filledQuery, limit)
    }

    override fun supportedDataSourceTypes(): Set<String> = setOf(
        "RDBMS_POSTGRESQL",
        "RDBMS_MYSQL",
        "RDBMS_MSSQLSERVER",
        "RDBMS_HSQL",
        "RDBMS_ORACLE",
        "RDBMS_DATA_WORLD",
    )
}
