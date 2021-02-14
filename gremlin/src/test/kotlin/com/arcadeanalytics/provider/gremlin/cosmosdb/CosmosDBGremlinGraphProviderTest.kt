package com.arcadeanalytics.provider.gremlin.cosmosdb

import com.arcadeanalytics.data.Sprite
import com.arcadeanalytics.data.SpritePlayer
import com.arcadeanalytics.provider.DataSourceGraphProvider
import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.IndexConstants.ARCADE_EDGE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_NODE_TYPE
import com.arcadeanalytics.provider.IndexConstants.ARCADE_TYPE
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.ArrayList

class CosmosDBGremlinGraphProviderTest {

    private lateinit var provider: DataSourceGraphProvider

    private lateinit var dataSource: DataSourceInfo

    @BeforeEach
    fun setup() {
        dataSource = DataSourceInfo(
            id = 1L,
            type = "GREMLIN_COSMOSDB",
            name = "testDataSource",
            server = "4c7bff3b-0ee0-4-231-b9ee.gremlin.cosmosdb.azure.com",
            port = 443,
            username = "\"/dbs/arcade/colls/arcade-graph\"",
            password = "arcade",
            database = this::class.java.simpleName
        )

        provider = CosmosDBGremlinGraphProvider()
    }

    @Test
    @Disabled
    fun shouldFetchAllVertexes() {

        val nodes = ArrayList<Sprite>()
        val edges = ArrayList<Sprite>()

        val indexer = object : SpritePlayer {
            override fun begin() {
                TODO("not implemented") // To change body of created functions use File | Settings | File Templates.
            }

            override fun processed(): Long {
                return 0
            }

            override fun play(document: Sprite) {

                when (document.valueOf(ARCADE_TYPE)) {
                    ARCADE_NODE_TYPE -> nodes.add(document)
                    ARCADE_EDGE_TYPE -> edges.add(document)
                }
                Assertions.assertThat(document.valueOf("@class"))
                    .isNotBlank()

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
            }

            override fun end() {
            }
        }

        provider.provideTo(dataSource, indexer)
        Assertions.assertThat(nodes).hasSize(7)
        Assertions.assertThat(edges).hasSize(10)
    }
}
