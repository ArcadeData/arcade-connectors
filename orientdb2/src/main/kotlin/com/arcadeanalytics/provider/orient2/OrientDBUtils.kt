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
package com.arcadeanalytics.provider.orient2

import com.arcadeanalytics.provider.DataSourceInfo
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.record.impl.ODocument

private val connectionTemplate = "remote:{server}:{port}/{database}"

fun createConnectionUrl(dataSource: DataSourceInfo): String =
    connectionTemplate
        .replace("{server}", dataSource.server)
        .replace("{port}", dataSource.port.toString())
        .replace("{database}", dataSource.database)

fun open(dataSource: DataSourceInfo): ODatabaseDocumentTx {
    val connectionUrl = createConnectionUrl(dataSource)
    val db = ODatabaseDocumentTx(connectionUrl)
    return db.open<ODatabaseDocumentTx>(dataSource.username, dataSource.password)
}

fun ODocument.isEdgeType(): Boolean = this.schemaClass.isEdgeType

fun ODocument.isVertexType(): Boolean = this.schemaClass.isVertexType

fun ODocument.type(): String =
    when {
        isEdgeType() -> "edge"
        isVertexType() -> "node"
        else -> "document"
    }
