package com.arcadeanalytics.provider.rdbms

import com.arcadeanalytics.provider.*
import com.arcadeanalytics.provider.rdbms.persistence.util.DBSourceConnection
import java.sql.ResultSet


class RDBMSTableDataProvider : DataSourceTableDataProvider {

    override fun fetchData(dataSource: DataSourceInfo, query: String, limit: Int): GraphData {

        DBSourceConnection.getConnection(dataSource).use { conn ->

            conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { stmt ->

                stmt.executeQuery(query).use { rs ->

                    val md = rs.metaData
                    val columns = md.columnCount
                    val cytoNodes = mutableSetOf<CytoData>()
                    var count: Long = 0
                    while (rs.next()) {
                        val row = mutableMapOf<String, Any>()
                        for (i in 1..columns) {
                            row[md.getColumnName(i)] = rs.getObject(i)
                        }

                        val data = Data(id = count++.toString(), record = row)

                        val cytoData = CytoData(classes = TABLE_CLASS,
                                group = "nodes",
                                data = data)

                        cytoNodes.add(cytoData)
                    }

                    val nodesProperties = mutableMapOf<String, TypeProperty>()
                    for (i in 1..columns) {

                        val columnName = md.getColumnName(i)

                        nodesProperties[columnName] = TypeProperty(name = columnName, type = mapType(md.getColumnTypeName(i)))
                    }

                    val tableClass = mutableMapOf<String, Any>()
                    tableClass["name"] = TABLE_CLASS
                    tableClass["cardinality"] = count
                    tableClass["properties"] = nodesProperties
                    val nodeClasses = mutableMapOf<String, MutableMap<String, Any>>()
                    nodeClasses[TABLE_CLASS] = tableClass;


                    val edgeClasses = mutableMapOf<String, MutableMap<String, Any>>()
                    val cytoEdges = emptySet<CytoData>()


                    return GraphData(nodeClasses, edgeClasses, cytoNodes, cytoEdges)

                }
            }
        }

        return GraphData.EMPTY
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
            "RDBMS_DATA_WORLD"
    )


}