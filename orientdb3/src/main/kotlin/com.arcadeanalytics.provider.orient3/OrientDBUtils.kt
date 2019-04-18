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
package com.arcadeanalytics.provider.orient3

import com.arcadeanalytics.provider.DataSourceInfo
import com.orientechnologies.orient.core.db.OrientDB
import com.orientechnologies.orient.core.db.OrientDBConfig
import com.orientechnologies.orient.core.db.document.ODatabaseDocument
import com.orientechnologies.orient.core.record.OEdge
import com.orientechnologies.orient.core.record.OVertex
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.executor.OResultSet
import java.util.*


private val orientdbConnectionUrl = "remote:{server}:{port}"


fun createOrientdbConnectionUrl(dataSource: DataSourceInfo): String {
    return orientdbConnectionUrl.replace("{server}", dataSource.server)
            .replace("{port}", dataSource.port.toString())
}

fun open(dataSource: DataSourceInfo): ODatabaseDocument {

    val orientdbConnectionUrl = createOrientdbConnectionUrl(dataSource);
    val orientDB = OrientDB(orientdbConnectionUrl, OrientDBConfig.defaultConfig())
    return orientDB.open(dataSource.name, dataSource.username, dataSource.password)
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

fun ODocument.isEdgeType(): Boolean {
    return this.schemaClass.isEdgeType
}

fun ODocument.isVertexType(): Boolean {
    return this.schemaClass.isVertexType
}

fun ODocument.type(): String = when {
    isEdgeType() -> "edge"
    isVertexType() -> "node"
    else -> "document"
}
