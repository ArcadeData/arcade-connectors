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

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.DataSourceGraphProvider
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.IndexConstants.ARCADE_EDGE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_ID
import com.arcadeanalytics.provider.IndexConstants.ARCADE_NODE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE
import com.arcadeanalytics.provider.gremlin.createSerializer
import com.google.common.collect.Sets
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.slf4j.LoggerFactory
import java.util.regex.Pattern

class CosmosDBGremlinGraphProvider : DataSourceGraphProvider {

    private val log = LoggerFactory.getLogger(CosmosDBGremlinGraphProvider::class.java)
    private val allFields: Pattern

    init {
        allFields = Pattern.compile(".*")
    }


    override fun supportedDataSourceTypes(): Set<String> {
        return Sets.newHashSet("GREMLIN_COSMOSDB")
    }

    override fun provideTo(dataSource: DataSourceInfo, player: SpritePlayer) {

        val cluster = Cluster.build(dataSource.server)
                .port(dataSource.port)
                .serializer(createSerializer(dataSource))
                .enableSsl(dataSource.type == "GREMLIN_COSMOSDB")
                .credentials(dataSource.username, dataSource.password)
                .create()


        val client = cluster.connect<Client>().init()

        provideNodes(dataSource, player, client)

        provideEdges(dataSource, player, client)

        client.close()

        cluster.close()
        player.end()
    }

    private fun provideNodes(dataSource: DataSourceInfo, processor: SpritePlayer, client: Client) {
        val nodes = client.submit("g.V().count()").one().long
        var fetched: Long = 0
        var skip: Long = 0
        var limit = Math.min(nodes, 1000)

        log.info("start indexing of data-source {} - total nodes:: {} ", dataSource.id, nodes)
        while (fetched < nodes) {

            val resultSet = client.submit("g.V().range($skip , $limit)")

            for (r in resultSet) {

                val res = r.getObject() as MutableMap<String, Any>

                if (res.containsKey("properties")) {

                    val props = res["properties"] as Map<String, List<Map<String, Any>>>

                    props.forEach { k, v -> if (v is List<*>) res.put(k, v[0]["value"]!!) }

                    res.remove("properties")
                    res.remove("class")
                }

                val sprite = Sprite()
                res.keys
                        .forEach { k -> sprite.add(k, res[k].toString()) }

                sprite.add(ARCADE_ID, dataSource.id.toString() + "_" + res["id"])
                        .add(ARCADE_TYPE, ARCADE_NODE_TYPE)
                        .add("@class", res["label"])
                        .apply<Any, String>(allFields) { v -> v.toString() }
                processor.play(sprite)
                fetched++
                log.info("fetched:: {} ", fetched)

            }

            skip = limit
            limit += 10000

        }
    }

    private fun provideEdges(dataSource: DataSourceInfo, processor: SpritePlayer, client: Client) {
        val edges = client.submit("g.E().count()").one().long
        var fetched: Long = 0
        var skip: Long = 0
        var limit = Math.min(edges, 1000)

        log.info("start indexing of data-source {} - total edges:: {} ", dataSource.id, edges)
        while (fetched < edges - 1) {

            log.info("query:: g.E().range($skip , $limit)")

            val resultSet = client.submit("g.E().range($skip , $limit)")

            for (r in resultSet) {

                val res = r.getObject() as MutableMap<String, Any>

                if (res.containsKey("properties")) {

                    val props = res["properties"] as Map<String, Any>
                    props.forEach { k, v -> res.put(k, v) }
                    res.remove("properties")
                    res.remove("class")
                }

                val sprite = Sprite()
                res.keys
                        .forEach { k -> sprite.add(k, res[k].toString()) }

                sprite.add(ARCADE_ID, dataSource.id.toString() + "_" + res["id"])
                        .add(ARCADE_TYPE, ARCADE_EDGE_TYPE)
                        .add("@class", res["label"])
                        .apply<Any, String>(allFields) { v -> v.toString() }
                processor.play(sprite)
                fetched++

                log.info("fetched:: {} ", fetched)
            }

            skip = limit
            limit += 10000

        }
    }
}
