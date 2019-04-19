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

class OrientDBDataSourceGraphProvider : DataSourceGraphProvider {

    private val log = LoggerFactory.getLogger(OrientDBDataSourceGraphProvider::class.java)

    private val queries: List<String>

    private val ALL_V = "SELECT FROM V LIMIT 1000"

    private val ALL_E = "SELECT FROM E LIMIT 1000"

    private val allFields: Pattern = Pattern.compile(".*")

    init {

        this.queries = listOf(ALL_V, ALL_E)
    }

    override fun supportedDataSourceTypes(): Set<String> {
        return setOf("ORIENTDB")
    }

    override fun provideTo(dataSource: DataSourceInfo, player: SpritePlayer) {

        open(dataSource).use { db ->

            queries.asSequence()
                    .onEach { query -> log.info("fetching documents with query ' {} ' ", query) }
                    .forEach { sql ->

                        var resultset = db.query(sql)

                        while (resultset.hasNext()) {

                            resultset.asSequence()
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
                                    .forEach { doc: Sprite -> player.play(doc) }

                            resultset = db.query(sql)

                        }
                        player.end()
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


