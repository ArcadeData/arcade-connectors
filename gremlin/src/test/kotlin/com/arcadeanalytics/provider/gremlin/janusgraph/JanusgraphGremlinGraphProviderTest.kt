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
package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.IndexConstants.ARCADE_EDGE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_NODE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE
import com.arcadeanalytics.provider.gremlin.janusgraph.JanusgraphContainer
import org.assertj.core.api.Assertions
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JanusgraphGremlinGraphProviderTest {


    private val provider: GremlinGraphProvider = GremlinGraphProvider()


    @Test
    fun shouldFetchAllVertexesAndEdges() {


        val nodes = ArrayList<Sprite>()
        val edges = ArrayList<Sprite>()

        val indexer = object : SpritePlayer {
            override fun begin() {
                //noop
            }

            override fun processed(): Long {
                return 0
            }


            override fun play(document: Sprite) {

                when (document.valueOf(ARCADE_TYPE)) {
                    ARCADE_NODE_TYPE -> nodes.add(document)
                    ARCADE_EDGE_TYPE -> edges.add(document)
                }
                Assertions.assertThat(document.valueOf("@class")).isNotBlank()
                Assertions.assertThat(document.hasField("_a_id")).isTrue()
                Assertions.assertThat(document.hasField("_a_type")).isTrue()
                Assertions.assertThat(document.hasField("_a_type")).isTrue()


            }

            override fun end() {

            }
        }

        provider.provideTo(JanusgraphContainer.dataSource, indexer)
        Assertions.assertThat(nodes).hasSize(808)
        Assertions.assertThat(edges).hasSize(7047)

    }


}
