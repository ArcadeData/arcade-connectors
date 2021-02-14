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
package com.arcadeanalytics.provider.orientdb

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.orient2.OrientDBContainer
import com.arcadeanalytics.provider.orient2.OrientDBDataSourceGraphProvider
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.ArrayList

/**
 * NOTE: tests are ignored because on our Jenkins the test containers isn't working
 * enable tests on local machine
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)

class OrientDBDataSourceGraphProviderIntTest {

    private val provider: OrientDBDataSourceGraphProvider = OrientDBDataSourceGraphProvider()

    @Test
    fun shouldFetchAllVerticesAndEdges() {

        val docs = ArrayList<Sprite>()

        val indexer = object : SpritePlayer {
            override fun begin() {
            }

            override fun end() {
            }

            override fun play(document: Sprite) {
                docs.add(document)
                assertThat(document.valuesOf("@class")).doesNotContain("V", "E")
            }
        }

        provider.provideTo(OrientDBContainer.dataSource, indexer)

        assertThat(docs).hasSize(8)
    }
}
