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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema
import org.slf4j.LoggerFactory

class OrientDBDataSourceMetadataProvider() : DataSourceMetadataProvider {


    private val log = LoggerFactory.getLogger(OrientDBDataSourceMetadataProvider::class.java)


    override fun fetchMetadata(dataSource: DataSourceInfo): DataSourceMetadata {


        open(dataSource).use {
            val schema = it.metadata.immutableSchemaSnapshot

            val nodesClasses = nodeClasses(schema, it)

            val edgeClasses = edgeClasses(schema, it)

            return DataSourceMetadata(nodesClasses, edgeClasses)
        }
    }

    private fun edgeClasses(schema: OImmutableSchema, db: ODatabaseDocumentTx): EdgesClasses {
        return schema.classes
                .asSequence()
                .filter { it.isEdgeType }
                .filter { it.name != "E" }
                .map {

                    val props = it.properties()
                            .map { prop -> prop.name to TypeProperty(prop.name, prop.type.name) }
                            .toMap()

                    TypeClass(it.name, db.countClass(it.name, true), props)

                }.map {
                    it.name to it
                }.toMap()
    }

    private fun nodeClasses(schema: OImmutableSchema, db: ODatabaseDocumentTx): NodesClasses {
        return schema.classes
                .asSequence()
                .filter { it.isVertexType }
                .filter { it.name != "V" }
                .map {

                    val props = it.properties()
                            .map { prop -> prop.name to TypeProperty(prop.name, prop.type.name) }
                            .toMap()

                    TypeClass(it.name, db.countClass(it.name, true), props)

                }.map {
                    it.name to it
                }.toMap()
    }


    override fun supportedDataSourceTypes(): Set<String> = setOf("ORIENTDB")

}
