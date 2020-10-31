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

package com.arcadeanalytics.provider.neo

import com.arcadeanalytics.provider.neo.Neo4jContainer.dataSource
import com.arcadeanalytics.provider.neo4j3.Neo4jDataProvider
import com.arcadeanalytics.provider.neo4j3.getDriver
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.entry
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.neo4j.driver.v1.AccessMode

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Neo4jDataProviderIntTest {

    private val provider: Neo4jDataProvider = Neo4jDataProvider()

    @Test
    @Disabled
    internal fun `should execute query`() {
        getDriver(dataSource).use {
            it.session(AccessMode.READ).use { session ->

                val query =
                    """MATCH (a)-[r]->(o)
                                        WITH a, o, type(r) as type
                                        RETURN a, type, count(type) as in_count
                                        ORDER BY a""".trimMargin()
                println("query = $query")

                for (record in session.run(query)) {

                    println("record = $record")
                }
            }
        }
    }

    @Test
    @Throws(Exception::class)
    fun testFetchData() {

        // given

        // when
        val query = "MATCH (people:Person)-[fof:FriendOf]-(friends) RETURN people, fof, friends"
        val data = provider.fetchData(dataSource, query, 100)

        // then
        assertThat(data.nodes).hasSize(4)
        assertThat(data.edges).hasSize(2)
        assertThat(data.nodesClasses).containsKeys("Person")
        assertThat(data.edgesClasses).containsKeys("FriendOf")

        val cytoData = data.nodes.first()
        assertThat(cytoData.data.record).isNotNull
            .containsKeys("name", "@edgeCount", "@in", "@out")

        println("cytoData = $cytoData")
        val record = cytoData.data.record
        assertThat(record).isNotNull
            .containsKeys("name", "@edgeCount", "@in", "@out")

        assertThat(record["@edgeCount"]).isEqualTo(2)
        assertThat(record["@in"] as Map<String, Int>).containsOnly(entry("HaterOf", 1))
        assertThat(record["@out"] as Map<String, Int>).containsOnly(entry("FriendOf", 1))
    }

    @Test
    @Throws(Exception::class)
    fun testFetchNodes() {

        // given

        // when
        val query = "MATCH (n) RETURN n;"
        val data = provider.fetchData(dataSource, query, 100)

        // then
        assertThat(data.nodes).hasSize(8)
        assertThat(data.nodesClasses).containsKeys("Person", "Car")
//        assertThat(data.edgesClasses).containsKeys("FriendOf", "HaterOf")

        data.nodes
            .onEach { println("it = $it") }
            .map { node -> node.data.record }
            .forEach { record ->
                assertThat(record)
                    .containsKeys("name", "@edgeCount", "@in", "@out")

                assertThat(record["@edgeCount"] as Int).isGreaterThanOrEqualTo(0)
                assertThat(record["@in"]).isNotNull
                assertThat(record["@out"]).isNotNull
            }
    }

    @Test
    @Throws(Exception::class)
    fun shouldTraverseFromGivenNode() {
        // given

        // when
        val data = provider.expand(dataSource, arrayOf("0,1"), "out", "FriendOf", 300)

        // then
        assertThat(data.nodes).hasSize(2)

        assertThat(data.edges).hasSize(1)

        assertThat(data.nodesClasses).containsKeys("Person")
        assertThat(data.edgesClasses).containsKeys("FriendOf")

        val cytoData = data.nodes.first()

        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.record["@edgeCount"].toString().toInt()).isGreaterThan(0)

        assertThat(cytoData.data.source).isEmpty()

        val record = cytoData.data.record
        assertThat(record).isNotNull
            .containsKeys("name", "@edgeCount", "@in", "@out")

        assertThat(record["@edgeCount"]).isEqualTo(2)
        assertThat(record["@in"] as Map<String, Int>).containsOnly(entry("HaterOf", 1))
        assertThat(record["@out"] as Map<String, Int>).containsOnly(entry("FriendOf", 1))
    }

    @Test
    fun shouldLoadFromClass() {
        val data = provider.loadFromClass(dataSource, "Person", 1)

        assertThat(data.nodes).hasSize(1)
        val cytoData = data.nodes.first()

        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.record["@edgeCount"].toString().toInt()).isGreaterThan(0)

        assertThat(cytoData.data.source).isEmpty()

        val record = cytoData.data.record
        assertThat(record).isNotNull
            .containsKeys("name", "@edgeCount", "@in", "@out")

        assertThat(record["@edgeCount"]).isEqualTo(2)
        assertThat(record["@in"] as Map<String, Int>).containsOnly(entry("HaterOf", 1))
        assertThat(record["@out"] as Map<String, Int>).containsOnly(entry("FriendOf", 1))
    }

    @Test
    fun shouldLoadEdgesOfExistingNodes() {
        val firstDataSet = provider.loadFromClass(dataSource, "Person", "name", "frank", 1)
        val secondDataSet = provider.loadFromClass(dataSource, "Person", "name", "john", 1)

        assertThat(firstDataSet.nodes).hasSize(1)
        assertThat(secondDataSet.nodes).hasSize(1)

        val firstNode = firstDataSet.nodes.first().data
        val secondNode = secondDataSet.nodes.first().data

        val edgeClasses = (firstNode.record["@in"] as Map<String, Int>).keys
            .union((firstNode.record["@out"] as Map<String, Int>).keys)
            .union((secondNode.record["@in"] as Map<String, Int>).keys)
            .union((secondNode.record["@out"] as Map<String, Int>).keys)

        val data = provider.edges(dataSource, arrayOf(firstNode.id), edgeClasses.toTypedArray(), arrayOf(secondNode.id))

        val cytoData = data.edges.first()

        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.source).isNotBlank()
        assertThat(cytoData.data.target).isNotBlank()

        assertThat(data.nodes).hasSize(2)
    }

    @Test
    fun shouldLoadFromClassWherePropertyHasValue() {
        val data = provider.loadFromClass(dataSource, "Person", "name", "frank", 10)

        assertThat(data.nodes).hasSize(1)

        val cytoData = data.nodes.first()
        println("cytoData = $cytoData")
        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.record["@edgeCount"].toString().toInt()).isGreaterThan(0)

        assertThat(cytoData.data.source).isEmpty()

        val record = cytoData.data.record
        assertThat(record).isNotNull
            .containsKeys("name", "@edgeCount", "@in", "@out")

        assertThat(record["@edgeCount"]).isEqualTo(2)
        assertThat(record["@out"] as Map<String, Int>).containsOnly(entry("HaterOf", 1))
        assertThat(record["@in"] as Map<String, Int>).containsOnly(entry("FriendOf", 1))
    }

    @Test
    @Throws(Exception::class)
    fun shouldTraverseAllEdgesGivenNode() {
        // given

        // when
        val data = provider.expand(dataSource, arrayOf("0,1"), "both", "", 300)

        // then
        assertThat(data.nodes).hasSize(3)
        assertThat(data.edges).hasSize(2)

        assertThat(data.nodesClasses).containsKeys("Person")
        assertThat(data.edgesClasses).containsKeys("FriendOf")

        val cytoData = data.nodes.stream().findFirst().get()

        assertThat(cytoData.data.record).isNotNull
        assertThat(cytoData.data.record["@edgeCount"]).isEqualTo(2)

        assertThat(cytoData.data.source).isEmpty()
    }
}
