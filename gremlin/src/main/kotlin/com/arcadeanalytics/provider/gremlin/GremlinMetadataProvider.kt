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
package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.DataSourceMetadata
import com.arcadeanalytics.provider.DataSourceMetadataProvider
import com.arcadeanalytics.provider.EdgesClasses
import com.arcadeanalytics.provider.NodesClasses
import com.arcadeanalytics.provider.TypeClass
import com.arcadeanalytics.provider.TypeProperties
import com.arcadeanalytics.provider.TypeProperty
import com.arcadeanalytics.provider.mapType
import org.apache.tinkerpop.gremlin.driver.Client
import org.slf4j.LoggerFactory

class GremlinMetadataProvider : DataSourceMetadataProvider {
    private val log = LoggerFactory.getLogger(GremlinMetadataProvider::class.java)

    override fun supportedDataSourceTypes() = setOf("GREMLIN_ORIENTDB", "GREMLIN_NEPTUNE", "GREMLIN_JANUSGRAPH")

    override fun fetchMetadata(dataSource: DataSourceInfo): DataSourceMetadata {
        log.info("fetching metadata for dataSource {} ", dataSource.id)

        val cluster = getCluster(dataSource)

        val client = cluster.connect<Client>().init()
        try {
            val nodeClasses = mapNodeClasses(client)
            val edgesClasses = mapEdgesClasses(client)

            return DataSourceMetadata(nodeClasses, edgesClasses)
        } finally {
            client.close()
            cluster.close()
        }
    }

    fun mapNodeClasses(client: Client): NodesClasses =
        client
            .submit("g.V().label().groupCount()")
            .asSequence()
            .onEach { tc -> println("tc = $tc") }
            .map { r -> r.`object` as Map<String, Long> }
            .flatMap { m -> m.entries.asSequence() }
            .map { TypeClass(it.key, it.value, mapNodeProperties(it.key, client)) }
            .map {
                it.name to it
            }.toMap()

    private fun mapNodeProperties(
        label: String,
        client: Client,
    ): TypeProperties =
        client
            .submit("""g.V().hasLabel(${splitMultilabel(label)}).limit(1).next()""")
            .asSequence()
            .flatMap { r -> r.element.properties<Any>().asSequence() }
            .map { property -> TypeProperty(property.key(), mapType(property.value().javaClass.simpleName)) }
            .map { it.name to it }
            .toMap()

    private fun mapEdgesClasses(client: Client): EdgesClasses =
        client
            .submit("g.E().label().dedup()")
            .asSequence()
            .map { r -> r.`object`.toString() }
            .map { TypeClass(it, countEdgeLabel(it, client), mapEdgeProperties(it, client)) }
            .map {
                it.name to it
            }.toMap()

    private fun mapEdgeProperties(
        label: String,
        client: Client,
    ): TypeProperties =
        client
            .submit("""g.E().hasLabel(${splitMultilabel(label)} ).limit(1).next()""")
            .asSequence()
            .flatMap { r -> r.element.properties<Any>().asSequence() }
            .map { property -> TypeProperty(property.key(), mapType(property.value().javaClass.simpleName)) }
            .map { it.name to it }
            .toMap()

    private fun countNodeLabel(
        label: String,
        client: Client,
    ): Long =
        client
            .submit("""g.V().hasLabel(${splitMultilabel(label)}).count()""")
            .one()
            .long

    private fun countEdgeLabel(
        label: String,
        client: Client,
    ): Long =
        client
            .submit("""g.E().hasLabel(${splitMultilabel(label)}).count()""")
            .one()
            .long
}
