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
package com.arcadeanalytics.provider.gremlin

import com.arcadeanalytics.provider.DataSourceGraphDataProvider
import com.arcadeanalytics.provider.gremlin.OrientDBGremlinContainer.dataSource
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GremlinDataProviderIntTest {

    private val provider: DataSourceGraphDataProvider

    init {
        provider = GremlinDataProvider()

    }

    @Test
    fun shouldFetchVertices() {

        val query = "g.V().limit(50) "

        val data = provider.fetchData(dataSource, query, 50)

        assertThat(data.nodes).hasSize(50)
        val cytoData = data.nodes.asSequence().first()

        assertThat(cytoData.data.source).isNullOrEmpty()
        assertThat(cytoData.data.target).isNullOrEmpty()

        val record = cytoData.data.record
        assertThat(record).isNotNull
                .containsKeys("@in", "@out", "Name", "@edgeCount")


    }

    @Test
    internal fun shouldFetchWithQuery() {
        val query = "g.V().hasLabel('Countries').has('Name', 'Italy').inE()"

        val data = provider.fetchData(dataSource, query, 50)

        assertThat(data.nodes).hasSize(5)
        assertThat(data.edges).hasSize(4)

        val aNode = data.nodes.first()

        assertThat(aNode.data.source).isNullOrEmpty()
        assertThat(aNode.data.target).isNullOrEmpty()

        val anEdge = data.edges.first()

        assertThat(anEdge.classes).isEqualTo("IsFromCountry")

    }

    @Test
    fun shouldLoadVerticesByIds() {

        assertThat(provider.testConnection(dataSource)).isTrue()

        val query = "g.V().limit(10) "

        val data = provider.fetchData(dataSource, query, 10)

        val ids = data.nodes.asSequence()
                .map { data -> data.data.id }
                .toList()

        val load = provider.load(dataSource, ids.toTypedArray())

        assertThat(load.nodes).hasSize(10)

    }

    @Test
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

        assertThat(load.nodes).hasSize(11)
        assertThat(load.edges).hasSize(6)

        load.edges.forEach {
            assertThat(ids).contains(it.data.target)

        }

    }

    @Test
    fun shouldTraverseAll() {

        val query = "g.V().limit(10) "

        val data = provider.fetchData(dataSource, query, 10)

        val ids = data.nodes.asSequence()
                .map { data -> data.data.id }
                .toList()

        val load = provider.expand(dataSource, ids.toTypedArray(), "both", "", 300)

        assertThat(load.nodes).hasSize(11)
        assertThat(load.edges).hasSize(6)

        load.edges.forEach {
            assertThat(ids).contains(it.data.target)

        }

    }


    @Test
    fun testFetchVerticesAndEdges() {


        val query = "g.V().bothE()limit(10)"

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
    fun shouldLoadEdgesOfExistingNodes() {
        val firstDataSet = provider.loadFromClass(dataSource, "Profiles", "Name", "Luca", 1)
        val secondDataSet = provider.loadFromClass(dataSource, "Profiles", "Name", "Colin", 1)

        assertThat(firstDataSet.nodes).hasSize(1)
        assertThat(secondDataSet.nodes).hasSize(1)

        val firstNode = firstDataSet.nodes.first().data
        val secondNode = secondDataSet.nodes.first().data

        val edgeClasses = (firstNode.record["@in"] as Map<String, Int>).keys
                .union((firstNode.record["@out"] as Map<String, Int>).keys)
                .union((secondNode.record["@in"] as Map<String, Int>).keys)
                .union((secondNode.record["@out"] as Map<String, Int>).keys)

        val data = provider.edges(dataSource, arrayOf(firstNode.id), edgeClasses.toTypedArray(), arrayOf(secondNode.id))


        println("data = ${data}")

        val cytoData = data.edges.first()

        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.source).isNotBlank()
        assertThat(cytoData.data.target).isNotBlank()

        assertThat(data.nodes).hasSize(2)

    }


    @Test
    fun shouldLoadFromClass() {
        val graphData = provider.loadFromClass(dataSource, "Countries", 10)

        assertThat(graphData.nodes).hasSize(10)

    }


    @Test
    fun shouldLoadFromClassWherePropertyHasValue() {
        val data = provider.loadFromClass(dataSource, "Countries", "Code", "AD", 10)

        assertThat(data.nodes).hasSize(1)

    }

}

