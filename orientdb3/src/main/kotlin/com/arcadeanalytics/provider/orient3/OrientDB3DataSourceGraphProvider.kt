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
package com.arcadeanalytics.provider.orient3

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.DataSourceGraphProvider
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.IndexConstants.ARCADE_ID
import com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE
import com.orientechnologies.orient.core.id.ORID
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.OElement
import com.orientechnologies.orient.core.record.impl.ODocument
import org.slf4j.LoggerFactory
import java.util.regex.Pattern

class OrientDB3DataSourceGraphProvider : DataSourceGraphProvider {

    private val log = LoggerFactory.getLogger(OrientDB3DataSourceGraphProvider::class.java)

    private val V_COUNT = "select count(*) as count from V"

    private val E_COUNT = "select count(*) as count from E"

    private val allFields: Pattern = Pattern.compile(".*")


    override fun supportedDataSourceTypes(): Set<String> {
        return setOf("ORIENTDB3")
    }

    override fun provideTo(dataSource: DataSourceInfo, player: SpritePlayer) {

        try {
            provide(dataSource, player, "V")
            provide(dataSource, player, "E")
        } finally {
            player.end()
        }

    }


    private fun provide(dataSource: DataSourceInfo, player: SpritePlayer, what: String) {

        val count: Long = open(dataSource).use { db ->
            db.query("select count(*) as count from $what").use { result ->
                result.asSequence().first().getProperty("count")
            }
        }

        var fetched: Long = 0
        var skip: ORID = ORecordId("#-1:-1")
        var lastORID: ORID = skip

        log.info("start indexing of '{}' from data-source {} - total :: {} ", what, dataSource.id, count)

        while (fetched < count) {

            open(dataSource).use { db ->
                db.query("SELECT * FROM $what WHERE @rid > $skip LIMIT 1000").use { resultSet ->
                    resultSet.asSequence()
                            .map { res -> res.element.get() }
                            .filter { elem -> elem.propertyNames.size > 0 }
                            .onEach { elem: OElement -> lastORID = elem.identity }
                            .map { elem: OElement -> elem as ODocument }
                            .map { doc -> toSprite(doc) }
                            .forEach { doc: Sprite ->
                                player.play(doc)
                                fetched++
                            }
                }
            }
            player.end()

            skip = lastORID
        }

    }


    private fun toSprite(document: ODocument): Sprite {
        val rid = document.identity

        return Sprite()
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
    }


}


