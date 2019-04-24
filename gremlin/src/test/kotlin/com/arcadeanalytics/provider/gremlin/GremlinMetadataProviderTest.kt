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

import org.assertj.core.api.Assertions
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)

class GremlinMetadataProviderTest {

    private val provider: GremlinMetadataProvider

    init {

        provider = GremlinMetadataProvider()

    }

    @Test
    fun fetchMetadata() {
        val metadata = provider.fetchMetadata(OrientDBGremlinContainer.dataSource)

        println("metadata = ${metadata}")

        Assertions.assertThat(metadata.nodesClasses)
                .hasSize(11)
                .containsKeys("Countries")

        Assertions.assertThat(metadata.nodesClasses["Countries"]!!.cardinality).isEqualTo(249)

        Assertions.assertThat(metadata.nodesClasses["Countries"]?.properties).containsKeys("Id", "Code", "Name")

        Assertions.assertThat(metadata.edgesClasses)
                .hasSize(9)
                .containsKeys("HasFriend")

        Assertions.assertThat(metadata.edgesClasses["HasFriend"]!!.cardinality).isEqualTo(1617)
    }

}

