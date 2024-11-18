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

import com.arcadeanalytics.provider.DataSourceInfo
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.db.ODatabaseType
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.executor.OResultSet
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory
import java.util.Optional

const val ORIENTDB3 = "ORIENTDB3"

private val orientdbConnectionUrl = "remote:{server}:{port}"

fun createOrientdbConnectionUrl(dataSource: DataSourceInfo): String =
    orientdbConnectionUrl
        .replace("{server}", dataSource.server)
        .replace("{port}", dataSource.port.toString())

fun open(dataSource: DataSourceInfo): ODatabaseSession {
    val orientdbConnectionUrl = createOrientdbConnectionUrl(dataSource)

    val orientDB = OrientDB(orientdbConnectionUrl, OrientDBConfig.defaultConfig())

    return orientDB.open(dataSource.database, dataSource.username, dataSource.password)
}

fun openGremlin(dataSource: DataSourceInfo): OrientGraph {
    val orientdbConnectionUrl = createOrientdbConnectionUrl(dataSource)
    val orientDB = OrientDB(orientdbConnectionUrl, OrientDBConfig.defaultConfig())
//    val session = orientDB.open(dataSource.database, dataSource.username, dataSource.password)

    val graphFactory = OrientGraphFactory(orientDB, dataSource.database, ODatabaseType.PLOCAL, dataSource.username, dataSource.password)

    return graphFactory.noTx
}

fun ODatabaseDocument.getVertex(document: ODocument): Optional<OVertex>? {
    val sql = "select * from V where @rid = ?"
    var result: OResultSet = this.query(sql, document.identity.toString())
    return result.asSequence().first().vertex
}

fun ODatabaseDocument.getEdge(document: ODocument): Optional<OEdge>? {
    val sql = "select * from E where @rid = ?"
    var result: OResultSet = this.query(sql, document.identity.toString())
    return result.asSequence().first().edge
}

fun ODocument.isEdgeType(): Boolean = this.schemaClass.isEdgeType

fun ODocument.isVertexType(): Boolean = this.schemaClass.isVertexType

fun ODocument.type(): String =
    when {
        isEdgeType() -> "edge"
        isVertexType() -> "node"
        else -> "document"
    }
