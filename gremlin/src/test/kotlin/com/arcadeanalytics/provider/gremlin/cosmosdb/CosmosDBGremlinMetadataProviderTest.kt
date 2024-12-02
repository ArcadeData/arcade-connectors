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
package com.arcadeanalytics.provider.gremlin.cosmosdb

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.DataSourceMetadataProvider
import com.arcadeanalytics.provider.gremlin.JanusgraphGremlinDataProviderIntTest
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class CosmosDBGremlinMetadataProviderTest {
    private lateinit var dataSource: DataSourceInfo

    private lateinit var provider: DataSourceMetadataProvider

    @BeforeEach
    fun setup() {
        dataSource =
            DataSourceInfo(
                id = 1L,
                type = "GREMLIN_COSMOSDB",
                name = "testDataSource",
                server = "4c7bff3b-0ee0-4-231-b9ee.gremlin.cosmosdb.azure.com",
                port = 443,
                username = "\"/dbs/arcade/colls/arcade-graph\"",
                password = "arcade",
                database = JanusgraphGremlinDataProviderIntTest::class.java.simpleName,
            )

        provider = CosmosDBGremlinMetadataProvider()
    }

    @Test
    @Disabled
    fun fetchMetadata() {
        val metadata = provider.fetchMetadata(dataSource)

        println("metadata = $metadata")

        Assertions
            .assertThat(metadata.nodesClasses)
            .hasSize(2)
            .containsKeys("Office")

        Assertions.assertThat(metadata.nodesClasses["Office"]!!.cardinality).isEqualTo(6)

        Assertions
            .assertThat(metadata.edgesClasses)
            .hasSize(2)
            .containsKeys("works_for")

        Assertions.assertThat(metadata.edgesClasses["works_for"]!!.cardinality).isEqualTo(6)
    }
}
