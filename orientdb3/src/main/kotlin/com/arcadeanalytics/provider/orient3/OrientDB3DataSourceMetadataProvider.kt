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
import com.arcadeanalytics.provider.DataSourceMetadata
import com.arcadeanalytics.provider.DataSourceMetadataProvider
import com.arcadeanalytics.provider.EdgesClasses
import com.arcadeanalytics.provider.NodesClasses
import com.arcadeanalytics.provider.TypeClass
import com.arcadeanalytics.provider.TypeProperties
import com.arcadeanalytics.provider.TypeProperty
import com.orientechnologies.orient.core.db.ODatabaseSession
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OSchema
import org.slf4j.LoggerFactory

class OrientDB3DataSourceMetadataProvider : DataSourceMetadataProvider {
    private val log = LoggerFactory.getLogger(OrientDB3DataSourceMetadataProvider::class.java)

    override fun fetchMetadata(dataSource: DataSourceInfo): DataSourceMetadata {
        log.info("fetching metadata for dataSource {} ", dataSource.id)

        open(dataSource).use {
            val schema = it.metadata.schema

            val nodesClasses = nodeClasses(schema, it)

            val edgeClasses = edgeClasses(schema, it)

            return DataSourceMetadata(nodesClasses, edgeClasses)
        }
    }

    private fun edgeClasses(
        schema: OSchema,
        db: ODatabaseSession,
    ): EdgesClasses =
        schema.classes
            .asSequence()
            .filter { isEdgeType(it) }
            .map { mapToType(it, db) }
            .map { it.name to it }
            .toMap()

    private fun nodeClasses(
        schema: OSchema,
        db: ODatabaseSession,
    ): NodesClasses =
        schema.classes
            .asSequence()
            .filter { isVertexType(it) }
            .map { mapToType(it, db) }
            .map { it.name to it }
            .toMap()

    private fun mapToType(
        oClass: OClass,
        db: ODatabaseSession,
    ): TypeClass {
        val props: TypeProperties =
            oClass
                .properties()
                .map { prop -> prop.name to TypeProperty(prop.name, prop.type.name) }
                .toMap()

        return TypeClass(oClass.name, db.countClass(oClass.name, true), props)
    }

    private fun isEdgeType(it: OClass) = it.isEdgeType && it.name != "E"

    private fun isVertexType(it: OClass) = it.isVertexType && it.name != "V"

    override fun supportedDataSourceTypes(): Set<String> = setOf("ORIENTDB3")
}
