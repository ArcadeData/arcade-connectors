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

import com.arcadeanalytics.provider.neo4j3.Neo4jMetadataProvider
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Neo4jMetadataProviderTest {
    private val provider: Neo4jMetadataProvider = Neo4jMetadataProvider()

    @Test
    fun fetchMetadata() {
        val metadata = provider.fetchMetadata(Neo4jContainer.dataSource)

        assertThat(metadata.nodesClasses)
            .hasSize(2)
            .containsKeys("Person", "Car")

        assertThat(metadata.nodesClasses["Person"]!!.cardinality).isEqualTo(4)

        assertThat(metadata.edgesClasses)
            .hasSize(2)
            .containsKeys("FriendOf", "HaterOf")

        assertThat(metadata.edgesClasses["FriendOf"]!!.cardinality).isEqualTo(2)
    }
}
