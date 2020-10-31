package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.DataSourceGraphProvider
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.IndexConstants.ARCADE_EDGE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_ID
import com.arcadeanalytics.provider.IndexConstants.ARCADE_NODE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE
import com.google.common.collect.Sets
import org.apache.commons.lang3.StringUtils
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.slf4j.LoggerFactory
import java.util.regex.Pattern
import kotlin.math.min

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

class GremlinGraphProvider : DataSourceGraphProvider {

    private val log = LoggerFactory.getLogger(GremlinGraphProvider::class.java)

    private val allFields: Pattern

    override fun provideTo(dataSource: DataSourceInfo, player: SpritePlayer) {
        val cluster = getCluster(dataSource)
        val client = cluster.connect<Client>().init()
        try {
            provideNodes(dataSource, player, client)
            provideEdges(dataSource, player, client)
            client.close()
        } finally {
            cluster.close()
            client.close()
            player.end()
        }
    }

    private fun provideNodes(dataSource: DataSourceInfo, processor: SpritePlayer, client: Client) {
        val nodes = client.submit("g.V().count()").one().long
        var fetched: Long = 0
        var skip: Long = 0
        var limit = min(nodes, 1000)
        log.info("start indexing of data-source {} - total nodes:: {} ", dataSource.id, nodes)
        while (fetched < nodes) {
            val resultSet = client.submit("g.V().range($skip , $limit)")
            for (r in resultSet) {
                val element = r.vertex
                val sprite = Sprite()
                element.keys()
                        .asSequence()
                        .flatMap { key: String? -> element.properties<Any>(key).asSequence() }
                        .forEach { v: VertexProperty<Any?> -> sprite.add(v.label(), v.value()) }

                sprite.add(ARCADE_ID, dataSource.id.toString() + "_" + cleanOrientId(element.id().toString()))
                        .add(ARCADE_TYPE, ARCADE_NODE_TYPE)
                        .add("@class", element.label())

                processor.play(sprite)
                fetched++
            }
            skip = limit
            limit += 10000
        }
    }

    private fun provideEdges(dataSource: DataSourceInfo, processor: SpritePlayer, client: Client) {
        val edges = client.submit("g.E().count()").one().long
        var fetched: Long = 0
        var skip: Long = 0
        var limit = min(edges, 1000)
        log.info("start indexing of data-source {} - total edges:: {} ", dataSource.id, edges)
        while (fetched < edges) {
            val resultSet = client.submit("g.E().range($skip , $limit)")
            for (r in resultSet) {
                val element = r.element
                if (element.keys().isNotEmpty()) {
                    val sprite = Sprite()
                    element.keys().asSequence()
                            .forEach { k: String? -> sprite.add(k!!, element.value<Any>(k).toString()) }

                    sprite.add(ARCADE_ID, dataSource.id.toString() + "_" + cleanOrientId(element.id().toString()))
                            .add(ARCADE_TYPE, ARCADE_EDGE_TYPE)
                            .add("@class", element.label())

                    processor.play(sprite)
                }
                fetched++
            }
            skip = limit
            limit += 10000
        }
    }

    private fun cleanOrientId(id: String): String {
        return StringUtils.removeStart(id, "#")
                .replace(":", "_")
    }

    override fun supportedDataSourceTypes(): Set<String> {
        return Sets.newHashSet("GREMLIN_ORIENTDB", "GREMLIN_NEPTUNE", "GREMLIN_JANUSGRAPH")
    }

    init {
        allFields = Pattern.compile(".*")
    }
}
