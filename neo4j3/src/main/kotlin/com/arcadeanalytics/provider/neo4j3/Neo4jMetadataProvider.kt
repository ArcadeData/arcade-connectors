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
package com.arcadeanalytics.provider.neo4j3

import com.arcadeanalytics.provider.*
import org.neo4j.driver.v1.*
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 *
 * @author Roberto Franchini
 */
class Neo4jMetadataProvider : DataSourceMetadataProvider {
    private val log = LoggerFactory.getLogger(Neo4jMetadataProvider::class.java)


    private val queries = mapOf(
            "NEO4J::LABELS" to "CALL db.labels() YIELD label",
            "NEO4J::EDGES" to "CALL db.relationshipTypes() YIELD relationshipType",

            "NEO4J_MEMGRAPH::LABELS" to "MATCH (n) UNWIND labels(n) AS label RETURN DISTINCT label",
            "NEO4J_MEMGRAPH::EDGES" to "MATCH ()-[r]->() RETURN DISTINCT type(r) AS relationshipType"
    )

    override fun supportedDataSourceTypes(): Set<String> = setOf("NEO4J", "NEO4J_MEMGRAPH")

    override fun fetchMetadata(dataSource: DataSourceInfo): DataSourceMetadata {

        val connectionUrl = createConnectionUrl(dataSource)

        log.info("fetching metadata for dataSource {} - {}", dataSource, connectionUrl)

        val config = Config.build()
                .withConnectionTimeout(5, TimeUnit.SECONDS)
                .withConnectionLivenessCheckTimeout(1L, TimeUnit.SECONDS)
                .toConfig()

        GraphDatabase.driver(connectionUrl, AuthTokens.basic(dataSource.username, dataSource.password), config)
                .use {
                    it.session(AccessMode.READ)
                            .use { session ->

                                val nodesClasses = nodeClasses(session, dataSource.type)

                                val edgeClasses = edgeClasses(session, dataSource.type)

                                return DataSourceMetadata(nodesClasses, edgeClasses)

                            }
                }
    }

    private fun nodeClasses(session: Session, type: String): NodesClasses {
        log.info("get metadata for nodes")
        return session
                .run(queries["$type::LABELS"])
                .list()
                .map { it.get("label").asString() }
                .map { TypeClass(it, countLabel(it, session), mapNodeProperties(it, session)) }
                .map {
                    it.name to it
                }.toMap()
    }


    private fun countLabel(label: String, session: Session): Long {
        val count = session.run("MATCH (n:$label) RETURN count(*) AS count")
        return count.single().get("count").asLong()
    }

    private fun mapNodeProperties(nodeClass: String, session: Session): TypeProperties {
        return session.run("MATCH (n:$nodeClass) RETURN distinct(keys(n))")
                .asSequence()
                .flatMap { record -> record.get("(keys(n))").asList().asSequence() }
                .map { property -> TypeProperty(property.toString(), "STRING") }
                .map { it.name to it }
                .toMap()
    }

    private fun edgeClasses(session: Session, type: String): EdgesClasses {
        log.info("get metadata for edges")
        return session
                .run(queries["$type::EDGES"])
                .list()
                .map { it.get("relationshipType").asString() }
                .map { TypeClass(it, countRelType(it, session), emptyMap()) }
                .map {
                    it.name to it
                }.toMap()
    }

    private fun countRelType(relType: String, session: Session): Long {
        val count = session.run("MATCH ()-[r:$relType]->() RETURN count(*) AS count")
        return count.single().get("count").asLong()
    }


    private fun mapRelationshipProperties(edgeClass: String, session: Session): TypeProperties {
        return session.run("MATCH ()-[r:$edgeClass]->() RETURN distinct(keys(r))")
                .asSequence()
                .map { record -> record.get("(keys(n))") }
                .filter { value -> !value.isNull }
                .flatMap { value -> value.asList().asSequence() }
                .map { property -> TypeProperty(property.toString(), "STRING") }
                .map { it.name to it }
                .toMap()
    }

}
