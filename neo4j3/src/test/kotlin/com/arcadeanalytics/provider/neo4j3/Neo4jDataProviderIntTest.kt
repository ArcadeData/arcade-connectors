package com.arcadeanalytics.provider.neo

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
        getDriver(Neo4jContainer.dataSource).use {
            it.session(AccessMode.READ).use { session ->

                val query = """MATCH (a)-[r]->(o)
                                        WITH a, o, type(r) as type
                                        RETURN a, type, count(type) as in_count
                                        ORDER BY a""".trimMargin()
                println("query = ${query}")

                for (record in session.run(query)) {

                    println("record = ${record}")
                }
            }
        }
    }


    @Test
    @Throws(Exception::class)
    fun testFetchData() {

        //given


        //when
        val query = "MATCH (people:Person)-[fof:FriendOf]-(friends) RETURN people, fof, friends"
        val data = provider.fetchData(Neo4jContainer.dataSource, query, 100)

        //then
        assertThat(data.nodes).hasSize(4)
        assertThat(data.edges).hasSize(2)
        assertThat(data.nodesClasses).containsKeys("Person")
        assertThat(data.edgesClasses).containsKeys("FriendOf")

        val cytoData = data.nodes.first()
        assertThat(cytoData.data.record).isNotNull
                .containsKeys("name", "@edgeCount", "@in", "@out")


        println("cytoData = ${cytoData}")
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

        //given


        //when
        val query = "MATCH (n) RETURN n;"
        val data = provider.fetchData(Neo4jContainer.dataSource, query, 100)

        //then
        assertThat(data.nodes).hasSize(8)
        assertThat(data.nodesClasses).containsKeys("Person", "Car")
//        assertThat(data.edgesClasses).containsKeys("FriendOf", "HaterOf")

        data.nodes
                .onEach { println("it = ${it}") }
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
        //given


        //when
        val data = provider.expand(Neo4jContainer.dataSource, arrayOf("0,1"), "out", "FriendOf", 300)


        //then
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
        val data = provider.loadFromClass(Neo4jContainer.dataSource, "Person", 1)

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
    fun shouldLoadFromClassWherePropertyHasValue() {
        val data = provider.loadFromClass(Neo4jContainer.dataSource, "Person", "name", "frank", 10)

        assertThat(data.nodes).hasSize(1)

        val cytoData = data.nodes.first()
        println("cytoData = ${cytoData}")
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
        //given


        //when
        val data = provider.expand(Neo4jContainer.dataSource, arrayOf("0,1"), "both", "", 300)

        //then
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
