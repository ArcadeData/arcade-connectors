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
package com.arcadeanalytics.provider.neo4j3

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.DataSourceGraphProvider
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.IndexConstants.ARCADE_ID
import com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE
import com.arcadeanalytics.provider.neo4j3.Neo4jDialect.NEO4J
import com.arcadeanalytics.provider.neo4j3.Neo4jDialect.NEO4J_MEMGRAPH
import com.google.common.collect.ImmutableMap
import org.neo4j.driver.v1.AccessMode
import org.neo4j.driver.v1.Session
import org.slf4j.LoggerFactory
import java.util.regex.Pattern

/**
 *
 * @author Roberto Franchini
 */

class Neo4jGraphProvider : DataSourceGraphProvider {

    private val log = LoggerFactory.getLogger(Neo4jGraphProvider::class.java)
    private val queries = mapOf(
            "NEO4J::LABELS" to "CALL db.labels() YIELD label",
            "NEO4J::EDGES" to "CALL db.relationshipTypes() YIELD relationshipType",

            "NEO4J_MEMGRAPH::LABELS" to "MATCH (n) UNWIND labels(n) AS label RETURN DISTINCT label",
            "NEO4J_MEMGRAPH::EDGES" to "MATCH ()-[r]->() RETURN DISTINCT type(r) AS relationshipType"
    )


    private val allFields: Pattern = Pattern.compile(".*")

    override fun provideTo(dataSource: DataSourceInfo,
                           player: SpritePlayer) {

        getDriver(dataSource).use { driver ->

            driver.session(AccessMode.READ).use { session ->

                indexNodes(dataSource, player, session)

                indexRelationships(dataSource, player, session)
            }
        }

    }

    private fun indexNodes(dataSource: DataSourceInfo,
                           processor: SpritePlayer,
                           session: Session) {

        val labels = session.run(queries["${dataSource.type}::LABELS"])

        labels.list()
                .asSequence()
                .map { res -> res.get("label").asString() }
                .forEach { label -> indexLabel(dataSource, processor, session, label) }

//        indexGame(dataSource, processor, session, "Game")

    }

    private fun indexRelationships(dataSource: DataSourceInfo,
                                   processor: SpritePlayer,
                                   session: Session) {

        val edges = countEdges(session)

        var skip = 0
        var limit = Math.min(edges, 10000)

        var fetched = 0
        while (fetched < edges) {

            val params = ImmutableMap.of<String, Any>("skip", skip, "limit", limit)

            log.info("fetching edges from '{}' with query '{}' and params {} - {}", session, "MATCH ()-[r]->() RETURN r SKIP \$skip LIMIT \$limit", skip, limit)

            val rels = session.run("MATCH ()-[r]->() RETURN r SKIP \$skip LIMIT \$limit", params)
            while (rels.hasNext()) {
                val record = rels.next()

                record.fields().asSequence()
                        .map { p -> p.value() }
                        .filter { v -> v.type().name() == "RELATIONSHIP" }
                        .map { v -> v.asRelationship() }
                        .filter { r -> r.size() > 0 }
                        .map { n ->
                            Sprite().load(n.asMap())
                                    .add("@class", n.type())
                                    .add(ARCADE_ID, toArcadeId(dataSource, Neo4jType.EDGE, n.id()))
                                    .add(ARCADE_TYPE, "edge")
                                    .apply<Any, String>(allFields) { v -> v.toString() }
                        }
                        .forEach { s -> processor.play(s) }
                fetched++

            }
            skip = limit
            limit += 10000

        }

        processor.end()
    }

    private fun indexLabel(dataSource: DataSourceInfo,
                           processor: SpritePlayer,
                           session: Session,
                           label: String) {
        val nodes = countNodes(session, label)
        log.info("fetching data from '{}' - for label {} total nodes '{}' ", session, label, nodes)

        var skip = 0
        var limit = Math.min(nodes, 10000)

        var fetched = 0
        while (fetched < nodes) {

            log.info("fetching data from '{}' with query ' {} ' ", session, """MATCH (n:$label) RETURN n SKIP $skip LIMIT $limit""")

            val result = session.run("""MATCH (n:$label) RETURN n SKIP $skip LIMIT $limit""")

            while (result.hasNext()) {
                val record = result.next()

                record.fields().asSequence()
                        .map { p -> p.value() }
                        .filter { v -> v.type().name() == "NODE" }
                        .map { v -> v.asNode() }
                        .map { n ->
                            Sprite().load(n.asMap())
                                    .addAll("@class", n.labels())
                                    .add(ARCADE_ID, toArcadeId(dataSource, Neo4jType.NODE, n.id()))
                                    .add(ARCADE_TYPE, "node")
                                    .apply(allFields, Any::toString)
                        }
                        .forEach { s -> processor.play(s) }

                fetched++
            }

            skip = limit
            limit += 10000
        }

        log.info("label '{}' indexed -  totals {}/{} ", label, fetched, nodes)
        processor.end()
    }


    private fun countNodes(session: Session, label: String): Int {
        val count = session.run("MATCH (n:$label) RETURN count(*) AS count")
                .single().get("count").asInt()
        log.info("nodes for label '{}' : {}", label, count)
        return count
    }

    private fun countEdges(session: Session): Int {
        val count = session.run("MATCH ()-[r]->() RETURN count(*) AS count")
                .single().get("count").asInt()
        log.info("edges count: {}", count)
        return count
    }


    override fun supportedDataSourceTypes(): Set<String> {
        return setOf(NEO4J.name, NEO4J_MEMGRAPH.name)
    }

}
