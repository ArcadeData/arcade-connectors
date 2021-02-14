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
package com.arcadeanalytics.provider.neo

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.neo4j3.Neo4jGraphProvider
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.ArrayList

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Neo4jGraphProviderTest {

    private val provider: Neo4jGraphProvider = Neo4jGraphProvider()

    @Test
    fun shouldFetchAllElements() {

        val docs = ArrayList<Sprite>()

        val indexer = object : SpritePlayer {
            override fun end() {
            }

            override fun begin() {
            }

            override fun processed(): Long {
                return 0
            }

            override fun play(document: Sprite) {
                docs.add(document)
            }
        }

        provider.provideTo(Neo4jContainer.dataSource, indexer)

        assertThat(docs).hasSize(12)
        assertThat(
            docs.asSequence()
                .map { s -> s.valueOf("@class") }
                .any { c -> c == "Person" || c == "Car" || c == "fraternal" || c == "killer" }
        ).isTrue()
    }
}
