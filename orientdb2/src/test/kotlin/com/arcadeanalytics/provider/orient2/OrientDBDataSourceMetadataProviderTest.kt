/*-
 * #%L
 * Arcade Connectors
 * %%
 * Copyright (C) 2018 - 2020 ArcadeData
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

import com.arcadeanalytics.provider.orient2.OrientDBDataSourceMetadataProvider
import com.arcadeanalytics.provider.orient2.OrientDBContainer.dataSource
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test


class OrientDBDataSourceMetadataProviderTest {


    private val provider: OrientDBDataSourceMetadataProvider = OrientDBDataSourceMetadataProvider()

    @Test
    fun shouldFetchMetadata() {

        val metadata = provider.fetchMetadata(dataSource)

        assertThat(metadata.nodesClasses)
                .hasSize(1)
                .containsKey("Person")

        assertThat(metadata.nodesClasses["Person"]!!.cardinality).isEqualTo(4)
        assertThat(metadata.nodesClasses["Person"]!!.properties.keys).contains("name")


        assertThat(metadata.edgesClasses)
                .hasSize(2)
                .containsKeys("FriendOf", "HaterOf")

        assertThat(metadata.edgesClasses["FriendOf"]!!.cardinality).isEqualTo(2)
        assertThat(metadata.edgesClasses["FriendOf"]!!.properties.keys).contains("kind")

        assertThat(metadata.edgesClasses["HaterOf"]!!.cardinality).isEqualTo(2)
        assertThat(metadata.edgesClasses["HaterOf"]!!.properties.keys).contains("kind")


    }
}
