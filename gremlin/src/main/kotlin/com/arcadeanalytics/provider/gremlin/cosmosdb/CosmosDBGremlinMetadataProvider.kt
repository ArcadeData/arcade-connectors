/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2020 ArcadeData
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
package com.arcadeanalytics.provider.gremlin.cosmosdb

import com.arcadeanalytics.provider.*
import com.arcadeanalytics.provider.gremlin.GremlinSerializerFactory.createSerializer
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.slf4j.LoggerFactory

class CosmosDBGremlinMetadataProvider() : DataSourceMetadataProvider {

    private val log = LoggerFactory.getLogger(CosmosDBGremlinMetadataProvider::class.java)

    override fun supportedDataSourceTypes(): Set<String> = setOf("GREMLIN_COSMOSDB")

    override fun fetchMetadata(datasource: DataSourceInfo): DataSourceMetadata {

        log.info("fetching metadata for dataSource {} ", datasource)

        val serializer = createSerializer(datasource)

        val cluster = Cluster.build(datasource.server)
                .port(datasource.port!!)
                .serializer(serializer)
                .enableSsl(datasource.type == "GREMLIN_COSMOSDB")
                .credentials(datasource.username, datasource.password)
                .maxWaitForConnection(10000)
                .create()

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

    fun mapNodeClasses(client: Client): NodesClasses {
        return client.submit("g.V().label().dedup()").asSequence()
                .map { r -> r.`object`.toString() }
                .map { TypeClass(it, countNodeLabel(it, client), mapNodeProperties(it, client)) }
                .map {
                    it.name to it
                }.toMap()
    }


    private fun mapNodeProperties(label: String, client: Client): TypeProperties {

        return client.submit("g.V().hasLabel('$label').limit(1)")
                .asSequence()
                .map { it -> it.`object` as Map<String, Any> }
                .map { it -> it["properties"] as Map<String, List<Map<String, Any>>> }
                .flatMap { it -> it.asSequence() }
                .map { it -> it.key }
                .filter { it != "id" }
                .map { property -> TypeProperty(property, "STRING") }
                .map { it.name to it }
                .toMap()
    }


    fun mapEdgesClasses(client: Client): EdgesClasses {
        return client.submit("g.E().label().dedup()").asSequence()
                .map { r -> r.`object`.toString() }
                .map { TypeClass(it, countEdgeLabel(it, client), mapEdgeProperties(it, client)) }
                .map {
                    it.name to it
                }.toMap()
    }

    private fun mapEdgeProperties(label: String, client: Client): TypeProperties {

        return client.submit("g.E().hasLabel('$label').limit(1)")
                .asSequence()
                .map { it -> it.`object` as Map<String, Any> }
                .flatMap { it -> it.keys.asSequence() }
                .map { property -> TypeProperty(property, "STRING") }
                .map { it.name to it }
                .toMap()
    }

    private fun countNodeLabel(label: String, client: Client): Long {
        return client.submit("g.V().hasLabel('$label').count()")
                .one().long

    }

    private fun countEdgeLabel(label: String, client: Client): Long {
        return client.submit("g.E().hasLabel('$label').count()")
                .one().long

    }

}
