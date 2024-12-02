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
package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.provider.gremlin.janusgraph.JanusgraphContainer.dataSource
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class JanusgraphGremlinDataProviderIntTest {
    private val provider: GremlinDataProvider = GremlinDataProvider()

    @Test
    fun shouldFetchVertices() {
        val query = "g.V().limit(50) "

        val data = provider.fetchData(dataSource, query, 50)

        assertThat(data.nodes).hasSize(50)
        val cytoData = data.nodes.asSequence().first()

        assertThat(cytoData.data.source).isNullOrEmpty()
        assertThat(cytoData.data.target).isNullOrEmpty()

        val record = cytoData.data.record
        assertThat(record)
            .isNotNull
            .containsKeys("@in", "@out", "name", "@edgeCount")
    }

    @Test
    fun shouldLoadVerticesByIds() {
        assertThat(provider.testConnection(dataSource)).isTrue()

        val query = "g.V().limit(10) "

        val data = provider.fetchData(dataSource, query, 10)

        val ids =
            data.nodes
                .asSequence()
                .map { it.data.id }
                .toList()

        val load = provider.load(dataSource, ids.toTypedArray())

        assertThat(load.nodes).hasSize(10)
    }

    @Test
    fun shouldTraverse() {
        val query = "g.V().limit(10) "

        val data = provider.fetchData(dataSource, query, 10)

        val ids =
            data.nodes
                .asSequence()
                .map { it.data.id }
                .toList()

        val label: String =
            data.nodes
                .asSequence()
                .map { it.data.record["@in"] as Map<String, Any> }
                .map { ins -> ins.keys }
                .flatMap { k -> k.asSequence() }
                .first()

        val load = provider.expand(dataSource, ids.toTypedArray(), "in", label, 50)

        assertThat(load.nodes).isNotEmpty
        assertThat(load.edges).isNotEmpty
    }

    @Test
    fun shouldTraverseAll() {
        val query = "g.V().limit(10) "

        val data = provider.fetchData(dataSource, query, 10)

        val ids =
            data.nodes
                .asSequence()
                .map { it.data.id }
                .toList()

        val load = provider.expand(dataSource, ids.toTypedArray(), "both", "", 50)

        assertThat(load.nodes).isNotEmpty
        assertThat(load.edges).isNotEmpty
    }

    @Test
    fun testFetchVerticesAndEdges() {
        val query = "g.V().bothE()limit(10)"

        val data = provider.fetchData(dataSource, query, 10)

        val cytoData = data.edges.asSequence().first()

        assertThat(cytoData.data.source).isNotNull()
        assertThat(cytoData.data.target).isNotNull()

        val record = cytoData.data.record
        assertThat(record)
            .isNotNull
            .containsKeys("@in", "@out")

        assertThat(data.nodes).isNotEmpty
    }

    @Test
    @Disabled
    fun shouldLoadEdgesOfExistingNodes() {
        val firstDataSet = provider.loadFromClass(dataSource, "artist", 100)
        val secondDataSet = provider.loadFromClass(dataSource, "song", 400)

//        assertThat(firstDataSet.nodes).hasSize(100)
//        assertThat(secondDataSet.nodes).hasSize(100)

        val firstNode = firstDataSet.nodes.first().data
        val secondNode = firstDataSet.nodes.elementAt(2).data

        val fromIds =
            firstDataSet.nodes
                .asSequence()
                .map { it.data.id }
                .toList()
        val toIds =
            secondDataSet.nodes
                .asSequence()
                .map { it.data.id }
                .toList()

        val edgeClasses =
            firstDataSet.nodes
                .union(secondDataSet.nodes)
                .asSequence()
                .map { d ->
                    (d.data.record["@in"] as Map<String, Int>)
                        .keys
                        .union((d.data.record["@out"] as Map<String, Int>).keys)
                }.flatMap { it.asSequence() }
                .toSet()

//        val edgeClasses = (firstNode.record["@in"] as Map<String, Int>).keys
//                .union((firstNode.record["@out"] as Map<String, Int>).keys)
//                .union((secondNode.record["@in"] as Map<String, Int>).keys)
//                .union((secondNode.record["@out"] as Map<String, Int>).keys)

        val data = provider.edges(dataSource, fromIds.toTypedArray(), edgeClasses.toTypedArray(), toIds.toTypedArray())

        println("data = $data")

        val cytoData = data.edges.first()

        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.source).isNotBlank()
        assertThat(cytoData.data.target).isNotBlank()

        assertThat(data.nodes).hasSize(20)
    }

    @Test
    fun shouldLoadFromClass() {
        val graphData = provider.loadFromClass(dataSource, "artist", 10)

        println("graphData = $graphData")
        assertThat(graphData.nodes).hasSize(10)
    }

    @Test
    fun shouldLoadFromClassWherePropertyHasValue() {
        val data = provider.loadFromClass(dataSource, "song", "songType", "original", 10)

        println("data = $data")
        assertThat(data.nodes).hasSize(10)
    }
}
