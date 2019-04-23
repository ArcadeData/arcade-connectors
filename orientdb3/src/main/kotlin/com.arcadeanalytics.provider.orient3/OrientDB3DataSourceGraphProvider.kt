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

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.DataSourceGraphProvider
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.IndexConstants.ARCADE_ID
import com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.record.impl.OEdgeDocument
import com.orientechnologies.orient.core.record.impl.OVertexDocument
import org.slf4j.LoggerFactory
import java.util.regex.Pattern

class OrientDB3DataSourceGraphProvider : DataSourceGraphProvider {

    private val log = LoggerFactory.getLogger(OrientDB3DataSourceGraphProvider::class.java)

    private val V_COUNT = "select count(*) as count from V"

    private val E_COUNT = "select count(*) as count from E"

    private val allFields: Pattern = Pattern.compile(".*")


    override fun supportedDataSourceTypes(): Set<String> {
        return setOf("ORIENTDB_3")
    }

    override fun provideTo(dataSource: DataSourceInfo, player: SpritePlayer) {

        try {
            provideNodes(dataSource, player)
            provideEdges(dataSource, player)
        } finally {
            player.end()
        }

    }

    private fun provideNodes(dataSource: DataSourceInfo, player: SpritePlayer) {

        open(dataSource).use { db ->

            val nodesCount: Long = db.query(V_COUNT).asSequence().iterator().next().getProperty("count")
            var fetched: Long = 0
            var skip: Long = 0
            var limit = Math.min(nodesCount, 1000)

            log.info("start indexing of data-source {} - total nodes:: {} ", dataSource.id, nodesCount)

            while (fetched < nodesCount) {

                val resultSet = db.query("select * from V skip $skip limit $limit")

                while (resultSet.hasNext()) {

                    resultSet.asSequence()
                            .map { res ->
                                res.element.get()
                            }
                            .filter { elem -> elem.propertyNames.size > 0 }
                            .map { elem: OElement ->

                                when {
                                    elem.isVertex() -> {
                                        elem as OVertexDocument
                                        toSprite(elem)
                                    }
                                    else -> {
                                        elem as OEdgeDocument
                                        toSprite(elem)
                                    }
                                }
                            }
                            .forEach {
                                doc: Sprite -> player.play(doc)
                                fetched++
                            }
                }
                player.end()

                skip = limit
                limit += 10000
            }

        }
    }

    private fun provideEdges(dataSource: DataSourceInfo, player: SpritePlayer) {

        open(dataSource).use { db ->

            val edgesCount: Long = db.query(E_COUNT).asSequence().iterator().next().getProperty("count")
            var fetched: Long = 0
            var skip: Long = 0
            var limit = Math.min(edgesCount, 1000)

            log.info("start indexing of data-source {} - total nodes:: {} ", dataSource.id, edgesCount)

            while (fetched < edgesCount) {

                val resultSet = db.query("select * from E skip $skip limit $limit")

                while (resultSet.hasNext()) {

                    resultSet.asSequence()
                            .map { res ->
                                res.element.get()
                            }
                            .filter { elem -> elem.propertyNames.size > 0 }
                            .map { elem: OElement ->

                                when {
                                    elem.isVertex() -> {
                                        elem as OVertexDocument
                                        toSprite(elem)
                                    }
                                    else -> {
                                        elem as OEdgeDocument
                                        toSprite(elem)
                                    }
                                }
                            }
                            .forEach {
                                doc: Sprite -> player.play(doc)
                                fetched++
                            }
                }
                player.end()

                skip = limit
                limit += 10000
            }

        }
    }

    private fun toSprite(document: ODocument): Sprite {
        val rid = document.identity

        val sprite = Sprite()
                .load(document.toMap())
                .addAll("@class",
                        document.schemaClass
                                .allSuperClasses
                                .asSequence()
                                .map { c -> c.name }
                                .toList()
                )
                .apply<Any, String>(allFields) { v -> v.toString() }
                .remove("@class", "V")
                .remove("@class", "E")
                .remove("@rid")
                .add(ARCADE_ID, "${rid.clusterId}_${rid.clusterPosition}")
                .add(ARCADE_TYPE, document.type())
        return sprite
    }


}


