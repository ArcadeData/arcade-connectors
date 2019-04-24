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
package com.arcadeanalytics.provider.gremlin.cosmosdb

import com.arcadeanalytics.provider.DataSourceInfo
import com.arcadeanalytics.provider.gremlin.JanusgraphGremlinDataProviderIntTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test

class CosmosDBGremlinDataProviderIntTest {

    private lateinit var dataSource: DataSourceInfo


    private lateinit var provider: CosmosDBGremlinDataProvider

    @BeforeEach
    fun setup() {


        dataSource = DataSourceInfo(id = 1L,
                type = "GREMLIN_COSMOSDB",
                name = "testDataSource",
                server = "YOUR SERVER",
                port = 443,
                username = "USERNAME",
                password = "PASSWORD",
                database = "N/A")

        provider = CosmosDBGremlinDataProvider()
    }


    @Test
    @Disabled
    fun shouldFetchVertices() {


        val query = "g.V().limit(50) "

        val data = provider.fetchData(dataSource, query, 10)

        assertThat(data.nodes).hasSize(7)
        val cytoData = data.nodes.asSequence().first()

        assertThat(cytoData.data.source).isNull()
        assertThat(cytoData.data.target).isNull()

        val record = cytoData.data.record
        assertThat(record).isNotNull
                .containsKeys("@in", "@out", "Name", "@edgeCount")

        println("cytoData = ${cytoData}")


    }

    @Test
    @Disabled
    fun shouldLoadVerticesByIds() {

        assertThat(provider.testConnection(dataSource)).isTrue()

        val query = "g.V().limit(5) "

        val data = provider.fetchData(dataSource, query, 5)

        val ids = data.nodes.asSequence()
                .map { data -> data.data.id }
                .toList()

        val load = provider.load(dataSource, ids.toTypedArray())

        assertThat(load.nodes).hasSize(5)

        load.nodes.asSequence().forEach { n -> println("n = ${n}") }
    }

    @Test
    @Disabled
    fun shouldTraverse() {

        val query = "g.V().limit(10) "

        val data = provider.fetchData(dataSource, query, 10)

        val ids = data.nodes.asSequence()
                .map { data -> data.data.id }
                .toList()

        val label: String = data.nodes.asSequence()
                .map { data -> data.data.record["@in"] as Map<String, Any> }
                .map { ins -> ins.keys }
                .flatMap { k -> k.asSequence() }
                .first()


        val load = provider.expand(dataSource, ids.toTypedArray(), "in", label, 300)

        assertThat(load.nodes).hasSize(6)
        assertThat(load.edges).hasSize(4)

    }

    @Test
    @Disabled
    fun shouldTraverseAll() {

        val query = "g.V().limit(10) "

        val data = provider.fetchData(dataSource, query, 10)

        val ids = data.nodes.asSequence()
                .map { data -> data.data.id }
                .toList()

        val load = provider.expand(dataSource, ids.toTypedArray(), "both", "", 300)

        assertThat(load.nodes).hasSize(7)
        assertThat(load.edges).hasSize(10)

    }


    @Test
    @Disabled
    fun testFetchVerticesAndEdges() {

        val query = "g.V().bothE().limit(10)"

        val data = provider.fetchData(dataSource, query, 10)

        val cytoData = data.edges.asSequence().first()

        assertThat(cytoData.data.source).isNotNull()
        assertThat(cytoData.data.target).isNotNull()

        val record = cytoData.data.record
        assertThat(record).isNotNull
                .containsKeys("@in", "@out")


        assertThat(data.nodes).isNotEmpty
    }


    @Test
    @Disabled
    fun shouldLoadFromClassLimited() {

        val graphData = provider.loadFromClass(dataSource, "Person", 1)

        assertThat(graphData.nodes).hasSize(1)

    }


}

