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
package com.arcadeanalytics.provider.gremlin.janusgraph

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.IndexConstants.ARCADE_EDGE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_NODE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE
import com.arcadeanalytics.provider.gremlin.GremlinGraphProvider
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.ArrayList

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class JanusgraphGremlinGraphProviderTest {
    private val provider: GremlinGraphProvider = GremlinGraphProvider()

    @Test
    fun shouldFetchAllVertexesAndEdges() {
        val nodes = ArrayList<Sprite>()
        val edges = ArrayList<Sprite>()

        val indexer =
            object : SpritePlayer {
                override fun begin() {
                    // noop
                }

                override fun processed(): Long = 0

                override fun play(sprite: Sprite) {
                    when (sprite.valueOf(ARCADE_TYPE)) {
                        ARCADE_NODE_TYPE -> nodes.add(sprite)
                        ARCADE_EDGE_TYPE -> edges.add(sprite)
                    }
                    assertThat(sprite.valueOf("@class")).isNotBlank
                    assertThat(sprite.hasField("_a_id")).isTrue
                    assertThat(sprite.hasField("_a_type")).isTrue
                    assertThat(sprite.hasField("_a_type")).isTrue
                }

                override fun end() {
                }
            }

        provider.provideTo(JanusgraphContainer.dataSource, indexer)
        assertThat(nodes).hasSize(808)
        assertThat(edges).hasSize(7047)
    }
}
