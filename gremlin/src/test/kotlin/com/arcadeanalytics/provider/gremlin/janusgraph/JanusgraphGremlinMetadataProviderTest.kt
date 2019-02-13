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
package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.provider.gremlin.janusgraph.JanusgraphContainer
import org.assertj.core.api.Assertions
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)

class JanusgraphGremlinMetadataProviderTest {


    private val provider: GremlinMetadataProvider = GremlinMetadataProvider()


    @Test
    fun fetchMetadata() {
        val metadata = provider.fetchMetadata(JanusgraphContainer.dataSource)

        println("metadata = ${metadata}")

        Assertions.assertThat(metadata.nodesClasses)
                .hasSize(2)
                .containsKeys("artist")

        Assertions.assertThat(metadata.nodesClasses["artist"]!!.cardinality).isEqualTo(224)

        Assertions.assertThat(metadata.nodesClasses["artist"]?.properties).containsKeys("name")

        Assertions.assertThat(metadata.edgesClasses)
                .hasSize(3)
                .containsKeys("followedBy")

        Assertions.assertThat(metadata.edgesClasses["followedBy"]!!.cardinality).isEqualTo(7047)
    }

}

